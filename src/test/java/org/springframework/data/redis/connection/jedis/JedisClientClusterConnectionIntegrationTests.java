/*
 * Copyright 2026-present the original author or authors.
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
import static org.springframework.data.redis.connection.ClusterTestVariables.*;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.test.condition.EnabledOnRedisClusterAvailable;
import org.springframework.data.redis.test.extension.JedisExtension;

import redis.clients.jedis.RedisClusterClient;

import java.util.Arrays;

/**
 * Integration tests for {@link JedisClientClusterConnection}.
 * <p>
 * These tests verify that the stub implementation correctly throws {@link UnsupportedOperationException}
 * for unimplemented operations and that basic lifecycle methods work as expected.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@EnabledOnRedisClusterAvailable
@ExtendWith(JedisExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class JedisClientClusterConnectionIntegrationTests {

	private JedisClientConnectionFactory factory;
	private JedisClientClusterConnection connection;

	@BeforeEach
	void setUp() {
		RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration();
		clusterConfig.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT));
		clusterConfig.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_2_PORT));
		clusterConfig.addClusterNode(new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_3_PORT));

		factory = new JedisClientConnectionFactory(clusterConfig);
		factory.afterPropertiesSet();
		factory.start();

		connection = (JedisClientClusterConnection) factory.getClusterConnection();
	}

	@AfterEach
	void tearDown() {
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
		if (factory != null) {
			factory.destroy();
		}
	}

	@Test // GH-XXXX
	void connectionShouldBeCreated() {
		assertThat(connection).isNotNull();
		assertThat(connection.getNativeConnection()).isNotNull();
		assertThat(connection.getNativeConnection()).isInstanceOf(RedisClusterClient.class);
	}

	@Test // GH-XXXX
	void isClosedShouldReturnFalseInitially() {
		assertThat(connection.isClosed()).isFalse();
	}

	@Test // GH-XXXX
	void closeShouldMarkConnectionAsClosed() {
		connection.close();
		assertThat(connection.isClosed()).isTrue();
	}

	@Test // GH-XXXX
	void pingShouldThrowUnsupportedOperationException() {
		RedisClusterNode node = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.ping(node))
				.withMessageContaining("Cluster operations not yet implemented");
	}

	@Test // GH-XXXX
	void keysShouldThrowUnsupportedOperationException() {
		RedisClusterNode node = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.keys(node, "*".getBytes()))
				.withMessageContaining("Cluster operations not yet implemented");
	}

	@Test // GH-XXXX
	void scanShouldThrowUnsupportedOperationException() {
		RedisClusterNode node = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.scan(node, ScanOptions.NONE))
				.withMessageContaining("Cluster operations not yet implemented");
	}

	@Test // GH-XXXX
	void randomKeyShouldThrowUnsupportedOperationException() {
		RedisClusterNode node = new RedisClusterNode(CLUSTER_HOST, MASTER_NODE_1_PORT);
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.randomKey(node))
				.withMessageContaining("Cluster operations not yet implemented");
	}

	@Test // GH-XXXX
	void executeShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.execute("GET", "key".getBytes(), Arrays.asList("arg".getBytes())))
				.withMessageContaining("Cluster operations not yet implemented");
	}

	@Test // GH-XXXX
	void geoCommandsShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.geoCommands())
				.withMessageContaining("Geo commands not yet implemented");
	}

	@Test // GH-XXXX
	void hashCommandsShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.hashCommands())
				.withMessageContaining("Hash commands not yet implemented");
	}

	@Test // GH-XXXX
	void stringCommandsShouldThrowUnsupportedOperationException() {
		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> connection.stringCommands())
				.withMessageContaining("String commands not yet implemented");
	}
}

