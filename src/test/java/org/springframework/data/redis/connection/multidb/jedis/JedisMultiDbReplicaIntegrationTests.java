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

import static org.assertj.core.api.Assertions.*;

import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.connection.multidb.MultiDbFailover;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.test.condition.EnabledOnRedisSentinelAvailable;

/**
 * Group S4 &mdash; OSS read-replica showcase for the Jedis driver (Mode M2). Demonstrates the two-factory wiring
 * pattern: a read-side {@link JedisConnectionFactory} backed by a multi-DB configuration over geo-distributed replicas,
 * plus a separate write-side factory pointing at the master.
 *
 * @author Tihomir Mateev
 */
@EnabledOnRedisSentinelAvailable
class JedisMultiDbReplicaIntegrationTests {

	private JedisConnectionFactory readFactory;
	private JedisConnectionFactory writeFactory;

	@BeforeEach
	void setUp() {

		// Read-side: multi-DB over the two read replicas (weights 1.0 / 0.5).
		readFactory = new JedisConnectionFactory(SettingsUtils.multiDbReplicaConfiguration());
		readFactory.afterPropertiesSet();
		readFactory.start();

		// Write-side: ordinary standalone factory aimed at the master. Writes must NOT go through the read factory.
		RedisStandaloneConfiguration master = new RedisStandaloneConfiguration(SettingsUtils.getHost(),
				SettingsUtils.getPort());
		writeFactory = new JedisConnectionFactory(master);
		writeFactory.afterPropertiesSet();
		writeFactory.start();
	}

	@AfterEach
	void tearDown() {
		readFactory.destroy();
		writeFactory.destroy();
	}

	@Test // MD-S4-1
	void writesViaMasterAreVisibleThroughTheMultiDbReadFactory() throws InterruptedException {

		StringRedisTemplate writeTemplate = new StringRedisTemplate(writeFactory);
		writeTemplate.afterPropertiesSet();

		StringRedisTemplate readTemplate = new StringRedisTemplate(readFactory);
		readTemplate.afterPropertiesSet();

		String key = "md:s4-1:" + UUID.randomUUID();
		writeTemplate.opsForValue().set(key, "geo-distributed");

		try {
			// Wait for asynchronous replication to converge before reading from the replica.
			Thread.sleep(500);

			assertThat(readTemplate.opsForValue().get(key)).isEqualTo("geo-distributed");
		} finally {
			writeTemplate.delete(key);
		}
	}

	@Test // MD-S4-2
	void readTemplateTransparentlyFollowsManualFailoverBetweenReplicas() throws InterruptedException {

		StringRedisTemplate writeTemplate = new StringRedisTemplate(writeFactory);
		writeTemplate.afterPropertiesSet();

		StringRedisTemplate readTemplate = new StringRedisTemplate(readFactory);
		readTemplate.afterPropertiesSet();

		String key = "md:s4-2:" + UUID.randomUUID();
		writeTemplate.opsForValue().set(key, "still-readable");
		Thread.sleep(500);

		try {
			// Read via the currently active replica ...
			String before = MultiDbFailover.jedisActiveEndpoint(readFactory);
			assertThat(readTemplate.opsForValue().get(key)).isEqualTo("still-readable");

			// ... fail over to the surviving replica; both replicas carry the master's data, so the read template
			// transparently follows the routing shift without reconfiguration.
			MultiDbFailover.jedisFailover(readFactory);
			assertThat(MultiDbFailover.jedisActiveEndpoint(readFactory))
					.as("manual failover should shift the active replica").isNotEqualTo(before);

			assertThat(readTemplate.opsForValue().get(key)).isEqualTo("still-readable");
		} finally {
			writeTemplate.delete(key);
		}
	}

	@Test // MD-S4-3
	void accidentalWriteThroughReadFactorySurfacesReadOnlyError() {

		StringRedisTemplate readTemplate = new StringRedisTemplate(readFactory);
		readTemplate.afterPropertiesSet();

		String key = "md:s4-3:" + UUID.randomUUID();

		assertThatExceptionOfType(DataAccessException.class) //
				.isThrownBy(() -> readTemplate.opsForValue().set(key, "should-fail")) //
				.satisfies(ex -> assertThat(ex).hasStackTraceContaining("READONLY"));

		// A second SET fails the same way; the multi-DB client must not get stuck in a failover loop on READONLY.
		assertThatExceptionOfType(DataAccessException.class) //
				.isThrownBy(() -> readTemplate.opsForValue().set(key, "should-fail-again")) //
				.satisfies(ex -> assertThat(ex).hasStackTraceContaining("READONLY"));
	}
}
