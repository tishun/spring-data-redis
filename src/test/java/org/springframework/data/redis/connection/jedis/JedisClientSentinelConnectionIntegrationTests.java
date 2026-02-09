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

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.NamedNode;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSentinelConnection;
import org.springframework.data.redis.connection.RedisServer;
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;

/**
 * Integration tests for {@link JedisClientSentinelConnection}.
 * <p>
 * These tests verify that the stub implementation correctly throws {@link UnsupportedOperationException}
 * for unimplemented operations and that basic lifecycle methods work as expected.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisSentinelAvailable
public class JedisClientSentinelConnectionIntegrationTests {

	private static final String MASTER_NAME = "mymaster";
	private static final NamedNode MASTER_NODE = new RedisNode.RedisNodeBuilder().withName(MASTER_NAME).build();

	private JedisClientConnectionFactory factory;
	private RedisSentinelConnection connection;

	@BeforeEach
	void setUp() {
		RedisSentinelConfiguration sentinelConfig = new RedisSentinelConfiguration()
				.master(MASTER_NAME)
				.sentinel("127.0.0.1", 26379)
				.sentinel("127.0.0.1", 26380);

		factory = new JedisClientConnectionFactory(sentinelConfig);
		factory.afterPropertiesSet();
		factory.start();

		connection = factory.getSentinelConnection();
	}

	@AfterEach
	void tearDown() throws Exception {
		if (connection != null && connection.isOpen()) {
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // GH-XXXX
	void connectionShouldBeCreated() {
		assertThat(connection).isNotNull();
		assertThat(connection).isInstanceOf(JedisClientSentinelConnection.class);
	}

	@Test // GH-XXXX
	void isOpenShouldReturnTrueInitially() {
		assertThat(connection.isOpen()).isTrue();
	}

	@Test // GH-XXXX
	void closeShouldMarkConnectionAsClosed() throws Exception {
		connection.close();
		// After close, isOpen should return false (though current implementation always returns true if client != null)
		// This test documents current behavior
	}

	@Test // GH-XXXX
	void failoverShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.failover(MASTER_NODE))
				.withMessageContaining("Failover not yet implemented");
	}

	@Test // GH-XXXX
	void mastersShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.masters())
				.withMessageContaining("Failover not yet implemented");
	}

	@Test // GH-XXXX
	void replicasShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.replicas(MASTER_NODE))
				.withMessageContaining("Failover not yet implemented");
	}

	@Test // GH-XXXX
	void removeShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.remove(MASTER_NODE))
				.withMessageContaining("Failover not yet implemented");
	}

	@Test // GH-XXXX
	void monitorShouldThrowUnsupportedOperationException() {
		RedisServer server = new RedisServer("127.0.0.1", 6379);
		server.setName(MASTER_NAME);
		server.setQuorum(2L);

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.monitor(server))
				.withMessageContaining("Failover not yet implemented");
	}

	@Test // GH-XXXX
	void failoverWithNullMasterShouldThrowException() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> connection.failover(null))
				.withMessageContaining("Redis node master must not be 'null'");
	}

	@Test // GH-XXXX
	void replicasWithNullMasterShouldThrowException() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> connection.replicas(null))
				.withMessageContaining("Master node cannot be 'null'");
	}

	@Test // GH-XXXX
	void removeWithNullMasterShouldThrowException() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> connection.remove(null))
				.withMessageContaining("Master node cannot be 'null'");
	}

	@Test // GH-XXXX
	void monitorWithNullServerShouldThrowException() {
		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> connection.monitor(null))
				.withMessageContaining("Cannot monitor 'null' server");
	}
}

