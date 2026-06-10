/*
 * Copyright 2025-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.multidb;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.failover.api.StatefulRedisMultiDbConnection;
import redis.clients.jedis.Endpoint;
import redis.clients.jedis.MultiDbClient;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;

/**
 * Manual, runtime failover helper for the multi-database integration showcases. Instead of injecting infrastructure
 * faults (container stop/start, {@code CLIENT PAUSE}), it drives each driver's native operator-style failover API to
 * shift routing between the configured endpoints while both nodes stay up. Automatic failure detection and
 * circuit-breaking are delegated to the driver test suites; these helpers keep the SDR tests focused on routing
 * transparency through the Spring Data abstractions.
 * <ul>
 * <li>Jedis: {@code (MultiDbClient) connection.getNativeConnection()} then {@link MultiDbClient#setActiveDatabase}.</li>
 * <li>Lettuce: the shared {@link StatefulRedisMultiDbConnection} reached via {@code getNativeConnection()} then
 * {@link StatefulRedisMultiDbConnection#switchTo(RedisURI)}.</li>
 * </ul>
 *
 * @author Tihomir Mateev
 */
public final class MultiDbFailover {

	private static final long HEALTH_TIMEOUT_MILLIS = 15_000;

	private MultiDbFailover() {}

	// ---- Jedis -------------------------------------------------------------------------------------

	public static String jedisActiveEndpoint(RedisConnectionFactory factory) {
		return describe(jedisClient(factory).getActiveDatabaseEndpoint());
	}

	/** Switch the Jedis multi-DB client to the first registered endpoint that is not currently active. */
	public static void jedisFailover(RedisConnectionFactory factory) {

		MultiDbClient client = jedisClient(factory);
		Endpoint active = client.getActiveDatabaseEndpoint();

		for (Endpoint endpoint : client.getDatabaseEndpoints()) {
			if (!endpoint.equals(active)) {
				awaitJedisHealthy(client, endpoint);
				client.setActiveDatabase(endpoint);
				return;
			}
		}
		throw new IllegalStateException("No alternate Jedis endpoint to fail over to");
	}

	private static MultiDbClient jedisClient(RedisConnectionFactory factory) {
		try (RedisConnection connection = factory.getConnection()) {
			return (MultiDbClient) connection.getNativeConnection();
		}
	}

	private static void awaitJedisHealthy(MultiDbClient client, Endpoint endpoint) {
		long deadline = System.currentTimeMillis() + HEALTH_TIMEOUT_MILLIS;
		while (!client.isHealthy(endpoint) && System.currentTimeMillis() < deadline) {
			sleep();
		}
	}

	private static String describe(Endpoint endpoint) {
		return endpoint.getHost() + ":" + endpoint.getPort();
	}

	// ---- Lettuce -----------------------------------------------------------------------------------

	public static String lettuceActiveEndpoint(RedisConnectionFactory factory) {
		RedisURI current = lettuceConnection(factory).getCurrentEndpoint();
		return current.getHost() + ":" + current.getPort();
	}

	/** Switch the shared Lettuce multi-DB connection to the first registered endpoint that is not currently active. */
	public static void lettuceFailover(RedisConnectionFactory factory) {

		StatefulRedisMultiDbConnection<byte[], byte[]> connection = lettuceConnection(factory);
		RedisURI current = connection.getCurrentEndpoint();

		for (RedisURI endpoint : connection.getEndpoints()) {
			if (!endpoint.equals(current)) {
				awaitLettuceHealthy(connection, endpoint);
				connection.switchTo(endpoint);
				return;
			}
		}
		throw new IllegalStateException("No alternate Lettuce endpoint to fail over to");
	}

	@SuppressWarnings("unchecked")
	private static StatefulRedisMultiDbConnection<byte[], byte[]> lettuceConnection(RedisConnectionFactory factory) {
		try (RedisConnection connection = factory.getConnection()) {
			RedisAsyncCommands<byte[], byte[]> async = (RedisAsyncCommands<byte[], byte[]>) connection
					.getNativeConnection();
			return (StatefulRedisMultiDbConnection<byte[], byte[]>) async.getStatefulConnection();
		}
	}

	private static void awaitLettuceHealthy(StatefulRedisMultiDbConnection<byte[], byte[]> connection, RedisURI endpoint) {
		long deadline = System.currentTimeMillis() + HEALTH_TIMEOUT_MILLIS;
		while (!connection.isHealthy(endpoint) && System.currentTimeMillis() < deadline) {
			sleep();
		}
	}

	private static void sleep() {
		try {
			Thread.sleep(100);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for endpoint health", ex);
		}
	}
}
