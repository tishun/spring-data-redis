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

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.condition.EnabledOnMultiDbAvailable;

/**
 * Group S3 &mdash; pub/sub integration over a multi-DB Lettuce factory. Both the imperative
 * {@link RedisMessageListenerContainer} and the {@link ReactiveRedisMessageListenerContainer} subscribe through a
 * multi-DB factory and receive messages published over the same factory. Failover detection and subscription rebinding
 * are driver concerns covered by the Lettuce test suite.
 *
 * @author Tihomir Mateev
 */
@EnabledOnMultiDbAvailable
class LettuceMultiDbPubSubIntegrationTests {

	@Test // MD-S3-1
	void messageListenerContainerDeliversOverMultiDbFactory() throws Exception {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.multiDbConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		String channel = "md:s3-1:" + UUID.randomUUID();
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(factory);
		container.setBeanName("lettuce-multidb-container");

		MessageListenerAdapter adapter = new MessageListenerAdapter(new Object() {
			@SuppressWarnings("unused")
			public void handleMessage(String message) {
				messages.add(message);
			}
		});
		adapter.afterPropertiesSet();
		container.addMessageListener(adapter, new ChannelTopic(channel));
		container.afterPropertiesSet();
		container.start();

		StringRedisTemplate publisher = new StringRedisTemplate(factory);
		publisher.afterPropertiesSet();

		try {
			String received = null;
			long deadline = System.currentTimeMillis() + 10_000;
			while (System.currentTimeMillis() < deadline && received == null) {
				publisher.convertAndSend(channel, "hello");
				received = messages.poll(500, TimeUnit.MILLISECONDS);
			}

			assertThat(received).as("listener should deliver over the multi-DB factory").isEqualTo("hello");
			assertThat(container.isRunning()).isTrue();
		} finally {
			container.destroy();
			factory.destroy();
		}
	}

	@Test // MD-S3-2
	void reactiveMessageListenerContainerDeliversOverMultiDbFactory() throws Exception {

		LettuceConnectionFactory factory = new LettuceConnectionFactory(SettingsUtils.multiDbConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		String channel = "md:s3-2:" + UUID.randomUUID();

		ReactiveRedisMessageListenerContainer container = new ReactiveRedisMessageListenerContainer(factory);
		ReactiveStringRedisTemplate publisher = new ReactiveStringRedisTemplate(factory);
		BlockingQueue<String> received = new LinkedBlockingQueue<>();
		reactor.core.Disposable subscription = container.receive(Topic.channel(channel)).map(m -> m.getMessage())
				.subscribe(received::add);

		try {
			// Republish while the reactive subscription handshake completes asynchronously.
			String delivered = null;
			long deadline = System.currentTimeMillis() + 10_000;
			while (System.currentTimeMillis() < deadline && delivered == null) {
				publisher.convertAndSend(channel, "hello").block();
				delivered = received.poll(500, TimeUnit.MILLISECONDS);
			}

			assertThat(delivered).as("reactive listener should deliver over the multi-DB factory").isEqualTo("hello");
		} finally {
			subscription.dispose();
			container.destroy();
			factory.destroy();
		}
	}
}
