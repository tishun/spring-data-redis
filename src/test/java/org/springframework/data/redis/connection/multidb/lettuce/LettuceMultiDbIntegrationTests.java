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
package org.springframework.data.redis.connection.multidb.lettuce;

import static org.assertj.core.api.Assertions.*;

import io.lettuce.core.failover.MultiDbClient;
import reactor.test.StepVerifier;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisMultiDbConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.multidb.MultiDbFailover;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * E2E showcase for client-side geographic failover (multi-database) over the Lettuce driver. Covers Groups S1, S2, and
 * S5 from the shared base, plus the Lettuce-only reactive variants (MD-S1-4 and MD-S2-3).
 *
 * @author Tihomir Mateev
 */
class LettuceMultiDbIntegrationTests
		extends org.springframework.data.redis.connection.multidb.AbstractMultiDbConnectionIntegrationTests {

	@Override
	protected RedisConnectionFactory createConnectionFactory(RedisMultiDbConfiguration configuration) {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(configuration);
		factory.afterPropertiesSet();
		factory.start();
		return factory;
	}

	@Override
	protected boolean isMultiDbAware(RedisConnectionFactory factory) {
		return ((LettuceConnectionFactory) factory).isMultiDbAware();
	}

	@Override
	protected Object getNativeMultiDbClient(RedisConnectionFactory factory) {

		// Lettuce exposes a typed convenience accessor for the multi-DB client. Users can drop down to the driver to
		// invoke failover-specific operations.
		MultiDbClient client = ((LettuceConnectionFactory) factory).getMultiDbClient();
		assertThat(client).isNotNull();
		return client;
	}

	@Override
	protected String activeEndpoint(RedisConnectionFactory factory) {
		return MultiDbFailover.lettuceActiveEndpoint(factory);
	}

	@Override
	protected void manualFailover(RedisConnectionFactory factory) {
		MultiDbFailover.lettuceFailover(factory);
	}

	// ---- Lettuce-only reactive showcases -------------------------------------------------------------

	@Test // MD-S1-4
	void reactiveRedisTemplateRoundTripsValuesOverMultiDb() {

		ReactiveStringRedisTemplate template = new ReactiveStringRedisTemplate(
				(LettuceConnectionFactory) connectionFactory);
		String key = "md:s1-4:" + UUID.randomUUID();

		template.opsForValue().set(key, "reactive-showcase") //
				.then(template.opsForValue().get(key)) //
				.as(StepVerifier::create) //
				.expectNext("reactive-showcase") //
				.verifyComplete();

		template.delete(key).block();
	}

	@Test // MD-S2-3
	void reactiveRedisTemplateOpsRemainTransparentAcrossManualFailover() {

		ReactiveStringRedisTemplate template = new ReactiveStringRedisTemplate(
				(LettuceConnectionFactory) connectionFactory);
		String keyPrefix = "md:s2-3:" + UUID.randomUUID() + ":";

		String before = activeEndpoint(connectionFactory);

		// Write against the active endpoint, fail over gracefully, then keep writing — reactively and transparently.
		template.opsForValue().set(keyPrefix + "before", "1") //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();

		manualFailover(connectionFactory);
		assertThat(activeEndpoint(connectionFactory)).as("manual failover should shift the active endpoint")
				.isNotEqualTo(before);

		template.opsForValue().set(keyPrefix + "after", "2") //
				.then(template.opsForValue().get(keyPrefix + "after")) //
				.as(StepVerifier::create) //
				.expectNext("2") //
				.verifyComplete();
	}
}
