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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.SettingsUtils;
import org.springframework.data.redis.connection.RedisMultiDbConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.test.condition.EnabledOnMultiDbAvailable;

/**
 * Scenario MD-S3-3 &mdash; demonstrates that a {@link RedisMessageListenerContainer} integrates with a
 * {@link RedisMultiDbConfiguration multi-DB} Jedis factory: the container subscribes through the multi-DB factory and a
 * {@link StringRedisTemplate} publishing over the same factory reaches the listener. Failover detection and
 * subscription rebinding are driver concerns covered by the Jedis test suite.
 *
 * @author Tihomir Mateev
 */
@EnabledOnMultiDbAvailable
class JedisMultiDbPubSubIntegrationTests {

	@Test // MD-S3-3
	void jedisPubSubDeliversOverMultiDbFactory() throws Exception {

		JedisConnectionFactory factory = new JedisConnectionFactory(SettingsUtils.multiDbConfiguration());
		factory.afterPropertiesSet();
		factory.start();

		String channel = "md:s3-3:" + UUID.randomUUID();
		BlockingQueue<String> messages = new LinkedBlockingQueue<>();

		RedisMessageListenerContainer container = new RedisMessageListenerContainer();
		container.setConnectionFactory(factory);
		container.setBeanName("jedis-multidb-container");

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
			// Publish via the multi-DB factory; the listener subscribed over the same factory receives the message.
			String delivered = null;
			long deadline = System.currentTimeMillis() + 10_000;
			while (System.currentTimeMillis() < deadline && delivered == null) {
				publisher.convertAndSend(channel, "hello");
				delivered = messages.poll(500, TimeUnit.MILLISECONDS);
			}

			assertThat(delivered).as("Jedis multi-DB pub/sub should deliver over the multi-DB factory").isEqualTo("hello");
			assertThat(container.isRunning()).isTrue();
		} finally {
			container.destroy();
			factory.destroy();
		}
	}
}
