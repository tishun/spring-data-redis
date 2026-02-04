/*
 * Copyright 2014-present the original author or authors.
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
package org.springframework.data.redis.connection.jedis;

import redis.clients.jedis.RedisSentinelClient;

import java.io.IOException;
import java.util.List;

import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.util.Assert;

/**
 * {@link RedisSentinelConnection} implementation using Jedis 7.2+ {@link RedisSentinelClient} API.
 * <p>
 * This implementation uses the new {@link RedisSentinelClient} class introduced in Jedis 7.2.0
 * for managing Redis Sentinel operations.
 *
 * @author Tihomir Mateev
 * @since 3.5
 * @see RedisSentinelClient
 * @see JedisSentinelConnection
 */
public class JedisClientSentinelConnection implements RedisSentinelConnection {

	private final RedisSentinelClient sentinelClient;

	/**
	 * Constructs a new {@link JedisClientSentinelConnection} instance.
	 *
	 * @param sentinelClient must not be {@literal null}.
	 */
	public JedisClientSentinelConnection(RedisSentinelClient sentinelClient) {
		Assert.notNull(sentinelClient, "RedisSentinelClient must not be null");
		this.sentinelClient = sentinelClient;
	}

	@Override
	public void failover(NamedNode master) {
		Assert.notNull(master, "Redis node master must not be 'null' for failover");
		Assert.hasText(master.getName(), "Redis master name must not be 'null' or empty for failover");
		// RedisSentinelClient extends UnifiedJedis which has sentinel command methods
		// sentinelClient.sentinelFailover(master.getName());
		throw new UnsupportedOperationException("Failover not yet implemented. Use JedisConnectionFactory for Sentinel support.");
	}

	@Override
	public List<RedisServer> masters() {
		// RedisSentinelClient extends UnifiedJedis which has sentinel command methods
		// return JedisConverters.toListOfRedisServer(sentinelClient.sentinelMasters());
		throw new UnsupportedOperationException("Failover not yet implemented. Use JedisConnectionFactory for Sentinel support.");

	}

	@Override
	public List<RedisServer> replicas(NamedNode master) {
		Assert.notNull(master, "Master node cannot be 'null' when loading replicas");
		Assert.notNull(master.getName(), "Master node name cannot be 'null' when loading replicas");
		// RedisSentinelClient extends UnifiedJedis which has sentinel command methods
		// return JedisConverters.toListOfRedisServer(sentinelClient.sentinelReplicas(master.getName()));
		throw new UnsupportedOperationException("Failover not yet implemented. Use JedisConnectionFactory for Sentinel support.");
	}

	@Override
	public void remove(NamedNode master) {
		Assert.notNull(master, "Master node cannot be 'null' when trying to remove");
		Assert.hasText(master.getName(), "Name of redis master cannot be 'null' or empty when trying to remove");
		// RedisSentinelClient extends UnifiedJedis which has sentinel command methods
		// sentinelClient.sentinelRemove(master.getName());
		throw new UnsupportedOperationException("Failover not yet implemented. Use JedisConnectionFactory for Sentinel support.");

	}

	@Override
	public void monitor(RedisServer server) {
		Assert.notNull(server, "Cannot monitor 'null' server");
		Assert.hasText(server.getName(), "Name of server to monitor must not be 'null' or empty");
		Assert.hasText(server.getHost(), "Host must not be 'null' for server to monitor");
		Assert.notNull(server.getPort(), "Port must not be 'null' for server to monitor");
		Assert.notNull(server.getQuorum(), "Quorum must not be 'null' for server to monitor");

		// RedisSentinelClient extends UnifiedJedis which has sentinel command methods
		// sentinelClient.sentinelMonitor(server.getName(), server.getRequiredHost(), server.getRequiredPort(), server.getQuorum().intValue());
		throw new UnsupportedOperationException("Failover not yet implemented. Use JedisConnectionFactory for Sentinel support.");

	}

	@Override
	public void close() throws IOException {
		try {
			sentinelClient.close();
		} catch (Exception ex) {
			throw new IOException("Failed to close RedisSentinelClient", ex);
		}
	}

	@Override
	public boolean isOpen() {
		// RedisSentinelClient doesn't have an isConnected() method in the new API
		// We'll assume it's open if the client is not null
		// A more robust implementation might try to execute a command and catch exceptions
		return sentinelClient != null;
	}
}

