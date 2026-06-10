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

import static org.assertj.core.api.Assertions.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.Person;
import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisMultiDbConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.test.condition.EnabledOnMultiDbAvailable;

/**
 * Shared E2E showcase scenarios for client-side geographic failover (multi-database) support: Groups S1, S2, and S5.
 * Driver-specific subclasses provide the connection factory under test.
 *
 * @author Tihomir Mateev
 */
@EnabledOnMultiDbAvailable
public abstract class AbstractMultiDbConnectionIntegrationTests {

	protected RedisConnectionFactory connectionFactory;

	/**
	 * Build the driver-specific {@link RedisConnectionFactory} from the given {@link RedisMultiDbConfiguration}. The
	 * implementation should call {@code afterPropertiesSet()} / {@code start()} so the returned factory is ready to use.
	 */
	protected abstract RedisConnectionFactory createConnectionFactory(RedisMultiDbConfiguration configuration);

	/** @return {@code true} when {@code factory.isMultiDbAware()} returns {@code true}. */
	protected abstract boolean isMultiDbAware(RedisConnectionFactory factory);

	/** @return the driver's native multi-DB client, exposed via the escape hatch documented in S5-2. */
	protected abstract Object getNativeMultiDbClient(RedisConnectionFactory factory);

	/**
	 * @return a {@code host:port} description of the endpoint the given factory currently routes to. Used to prove that
	 *         a {@link #manualFailover(RedisConnectionFactory) manual failover} actually shifted the active endpoint.
	 */
	protected abstract String activeEndpoint(RedisConnectionFactory factory);

	/**
	 * Perform a graceful, operator-style failover by driving the driver's native manual-failover API to switch routing
	 * to the registered endpoint that is not currently active. Both endpoints stay up; this exercises routing
	 * transparency at the Spring Data level without infrastructure faults.
	 */
	protected abstract void manualFailover(RedisConnectionFactory factory);

	@BeforeEach
	void setUp() {
		this.connectionFactory = createConnectionFactory(SettingsUtils.multiDbConfiguration());
	}

	@AfterEach
	void tearDown() throws Exception {
		if (connectionFactory instanceof DisposableBean disposable) {
			disposable.destroy();
		}
	}

	// ---- Group S1 — Wiring showcase ------------------------------------------------------------------

	@Test // MD-S1-1
	void connectionFactoryFromMultiDbConfigurationPingsTheActiveEndpoint() {

		assertThat(isMultiDbAware(connectionFactory)).isTrue();

		try (RedisConnection connection = connectionFactory.getConnection()) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}
	}

	@Test // MD-S1-2
	void stringRedisTemplateRoundTripsValuesOverMultiDb() {

		StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		String key = "md:s1-2:" + UUID.randomUUID();
		template.opsForValue().set(key, "showcase");

		assertThat(template.opsForValue().get(key)).isEqualTo("showcase");
		assertThat(template.delete(key)).isTrue();
	}

	@Test // MD-S1-3
	void redisTemplateRoundTripsHashEntriesOverMultiDb() {

		RedisTemplate<String, Object> template = new RedisTemplate<>();
		template.setConnectionFactory(connectionFactory);
		template.afterPropertiesSet();

		String key = "md:s1-3:" + UUID.randomUUID();
		Map<String, Person> entries = new HashMap<>();
		entries.put("homer", new Person("Homer", "Simpson", 39));
		entries.put("marge", new Person("Marge", "Simpson", 36));

		template.opsForHash().putAll(key, entries);

		Map<Object, Object> readBack = template.opsForHash().entries(key);
		assertThat(readBack).hasSize(2).containsValue(entries.get("homer")).containsValue(entries.get("marge"));

		template.delete(key);
	}

	// ---- Group S2 — Failover transparency ------------------------------------------------------------

	@Test // MD-S2-1
	void stringRedisTemplateOpsRemainTransparentAcrossManualFailover() {

		StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		String keyPrefix = "md:s2-1:" + UUID.randomUUID() + ":";

		String before = activeEndpoint(connectionFactory);
		template.opsForValue().set(keyPrefix + "before", "1");

		// Graceful operator-style failover to the other region; both endpoints stay up.
		manualFailover(connectionFactory);

		String after = activeEndpoint(connectionFactory);
		assertThat(after).as("manual failover should shift the active endpoint").isNotEqualTo(before);

		// The template transparently follows the new active endpoint — no reconfiguration required.
		template.opsForValue().set(keyPrefix + "after", "2");
		assertThat(template.opsForValue().get(keyPrefix + "after")).isEqualTo("2");
	}

	@Test // MD-S2-2
	void stringRedisTemplateOpsSurviveFailbackToTheOriginalEndpoint() {

		StringRedisTemplate template = new StringRedisTemplate(connectionFactory);
		template.afterPropertiesSet();

		String keyPrefix = "md:s2-2:" + UUID.randomUUID() + ":";
		String origin = activeEndpoint(connectionFactory);

		// Fail away from the original endpoint, write against the new one ...
		manualFailover(connectionFactory);
		assertThat(activeEndpoint(connectionFactory)).isNotEqualTo(origin);
		template.opsForValue().set(keyPrefix + "failover", "1");

		// ... then fail back to the original endpoint and confirm the template keeps working.
		manualFailover(connectionFactory);
		assertThat(activeEndpoint(connectionFactory)).as("a second failover should fail back to the origin")
				.isEqualTo(origin);

		template.opsForValue().set(keyPrefix + "failback", "2");
		assertThat(template.opsForValue().get(keyPrefix + "failback")).isEqualTo("2");
	}

	// ---- Group S5 — Lifecycle and escape hatch -------------------------------------------------------

	@Test // MD-S5-1
	void destroyOnMultiDbBackedFactoryShutsDownCleanly() throws Exception {

		try (RedisConnection connection = connectionFactory.getConnection()) {
			assertThat(connection.ping()).isEqualTo("PONG");
		}

		((DisposableBean) connectionFactory).destroy();

		assertThatExceptionOfType(Exception.class).isThrownBy(() -> connectionFactory.getConnection().close());

		// prevent double-destroy in @AfterEach
		this.connectionFactory = createConnectionFactory(SettingsUtils.multiDbConfiguration());
	}

	@Test // MD-S5-2
	void nativeMultiDbClientEscapeHatchIsReachableFromUserCode() {

		Object nativeClient = getNativeMultiDbClient(connectionFactory);

		assertThat(nativeClient).as("driver-native multi-DB client must be reachable for advanced use cases").isNotNull();
	}
}
