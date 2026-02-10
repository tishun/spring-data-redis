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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Unit tests for {@link JedisClientConnectionFactory}.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
class JedisClientConnectionFactoryUnitTests {

	private JedisClientConnectionFactory connectionFactory;

	@AfterEach
	void tearDown() {
		if (connectionFactory != null) {
			connectionFactory.destroy();
		}
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithDefaultConfiguration() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory).isNotNull();
		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getStandaloneConfiguration().getHostName()).isEqualTo("localhost");
		assertThat(connectionFactory.getStandaloneConfiguration().getPort()).isEqualTo(6379);
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithStandaloneConfiguration() {

		RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("redis-host", 6380);
		config.setDatabase(5);
		config.setPassword(RedisPassword.of("secret"));

		connectionFactory = new JedisClientConnectionFactory(config);

		assertThat(connectionFactory.getStandaloneConfiguration()).isNotNull();
		assertThat(connectionFactory.getStandaloneConfiguration().getHostName()).isEqualTo("redis-host");
		assertThat(connectionFactory.getStandaloneConfiguration().getPort()).isEqualTo(6380);
		assertThat(connectionFactory.getStandaloneConfiguration().getDatabase()).isEqualTo(5);
		assertThat(connectionFactory.getStandaloneConfiguration().getPassword()).isEqualTo(RedisPassword.of("secret"));
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithSentinelConfiguration() {

		RedisSentinelConfiguration config = new RedisSentinelConfiguration()
				.master("mymaster")
				.sentinel("127.0.0.1", 26379)
				.sentinel("127.0.0.1", 26380);

		connectionFactory = new JedisClientConnectionFactory(config);

		assertThat(connectionFactory.getSentinelConfiguration()).isNotNull();
		assertThat(connectionFactory.getSentinelConfiguration().getMaster().getName()).isEqualTo("mymaster");
		assertThat(connectionFactory.getSentinelConfiguration().getSentinels()).hasSize(2);
	}

	@Test // GH-XXXX
	void shouldCreateFactoryWithClusterConfiguration() {

		RedisClusterConfiguration config = new RedisClusterConfiguration()
				.clusterNode("127.0.0.1", 7000)
				.clusterNode("127.0.0.1", 7001)
				.clusterNode("127.0.0.1", 7002);

		connectionFactory = new JedisClientConnectionFactory(config);

		assertThat(connectionFactory.getClusterConfiguration()).isNotNull();
		assertThat(connectionFactory.getClusterConfiguration().getClusterNodes()).hasSize(3);
	}

	@Test // GH-XXXX
	void shouldNotBeStartedInitially() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory.isRunning()).isFalse();
	}

	@Test // GH-XXXX
	void shouldBeRunningAfterStart() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();

		assertThat(connectionFactory.isRunning()).isTrue();
	}

	@Test // GH-XXXX
	void shouldNotBeRunningAfterStop() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.afterPropertiesSet();
		connectionFactory.start();
		connectionFactory.stop();

		assertThat(connectionFactory.isRunning()).isFalse();
	}

	@Test // GH-XXXX
	void shouldSupportAutoStartup() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory.isAutoStartup()).isTrue();
	}

	@Test // GH-XXXX
	void shouldAllowDisablingAutoStartup() {

		connectionFactory = new JedisClientConnectionFactory();
		connectionFactory.setAutoStartup(false);

		assertThat(connectionFactory.isAutoStartup()).isFalse();
	}

	@Test // GH-XXXX
	void shouldSupportEarlyStartup() {

		connectionFactory = new JedisClientConnectionFactory();

		assertThat(connectionFactory.isEarlyStartup()).isTrue();
	}
}

