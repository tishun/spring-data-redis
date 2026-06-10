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
package org.springframework.data.redis.connection.multidb.jedis;

import redis.clients.jedis.MultiDbClient;

import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisMultiDbConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.multidb.AbstractMultiDbConnectionIntegrationTests;
import org.springframework.data.redis.connection.multidb.MultiDbFailover;

/**
 * E2E showcase for client-side geographic failover (multi-database) over the Jedis driver. Covers Groups S1, S2, and
 * S5 of the integration test plan; pub/sub and read-replica showcases live in sibling classes.
 *
 * @author Tihomir Mateev
 */
class JedisMultiDbIntegrationTests extends AbstractMultiDbConnectionIntegrationTests {

	@Override
	protected RedisConnectionFactory createConnectionFactory(RedisMultiDbConfiguration configuration) {

		JedisConnectionFactory factory = new JedisConnectionFactory(configuration);
		factory.afterPropertiesSet();
		factory.start();
		return factory;
	}

	@Override
	protected boolean isMultiDbAware(RedisConnectionFactory factory) {
		return ((JedisConnectionFactory) factory).isMultiDbAware();
	}

	@Override
	protected Object getNativeMultiDbClient(RedisConnectionFactory factory) {

		// Jedis exposes the multi-DB client through any RedisConnection: getNativeConnection() returns the
		// UnifiedJedis which is a MultiDbClient in multi-database mode. Users cast to MultiDbClient to access
		// failover operations such as setActiveDatabase(...).
		try (RedisConnection connection = factory.getConnection()) {
			return (MultiDbClient) connection.getNativeConnection();
		}
	}

	@Override
	protected String activeEndpoint(RedisConnectionFactory factory) {
		return MultiDbFailover.jedisActiveEndpoint(factory);
	}

	@Override
	protected void manualFailover(RedisConnectionFactory factory) {
		MultiDbFailover.jedisFailover(factory);
	}
}
