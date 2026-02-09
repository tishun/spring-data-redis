/*
 * Copyright 2024-present the original author or authors.
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.redis.connection.AbstractConnectionIntegrationTests;
import org.springframework.data.redis.test.condition.EnabledOnRedisVersion;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Integration test of {@link JedisClientConnection}
 * <p>
 * These tests require Redis 7.2+ to be available.
 *
 * @author Tihomir Mateev
 * @since 4.1
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class JedisClientConnectionIntegrationTests extends AbstractConnectionIntegrationTests {

	@AfterEach
	public void tearDown() {
		try {
			connection.flushAll();
		} catch (Exception ignore) {
			// Jedis leaves some incomplete data in OutputStream on NPE caused by null key/value tests
			// Attempting to flush the DB or close the connection will result in error on sending QUIT to Redis
		}

		try {
			connection.close();
		} catch (Exception ignore) {}

		connection = null;
	}

	@Test
	void shouldSetAndGetValue() {
		connection.set("key", "value");
		assertThat(connection.get("key")).isEqualTo("value");
	}

	@Test
	void shouldHandlePipeline() {
		connection.openPipeline();
		connection.set("key1", "value1");
		connection.set("key2", "value2");
		connection.get("key1");
		connection.get("key2");
		
		var results = connection.closePipeline();
		
		assertThat(results).hasSize(4);
		assertThat(results.get(2)).isEqualTo("value1");
		assertThat(results.get(3)).isEqualTo("value2");
	}

	@Test
	void shouldHandleTransaction() {
		connection.multi();
		connection.set("txKey1", "txValue1");
		connection.set("txKey2", "txValue2");
		connection.get("txKey1");
		
		var results = connection.exec();
		
		assertThat(results).isNotNull();
		assertThat(results).hasSize(3);
		assertThat(results.get(2)).isEqualTo("txValue1");
	}

	@Test
	void shouldGetClientName() {
		assertThat(connection.getClientName()).isEqualTo("jedis-client-test");
	}

	@Test
	void shouldSelectDatabase() {
		connection.select(1);
		connection.set("dbKey", "dbValue");
		
		connection.select(0);
		assertThat(connection.get("dbKey")).isNull();
		
		connection.select(1);
		assertThat(connection.get("dbKey")).isEqualTo("dbValue");
		
		// Clean up
		connection.del("dbKey");
		connection.select(0);
	}

	@Test
	void shouldHandleWatchUnwatch() {
		connection.set("watchKey", "initialValue");
		
		connection.watch("watchKey".getBytes());
		connection.multi();
		connection.set("watchKey", "newValue");
		
		var results = connection.exec();
		
		assertThat(results).isNotNull();
		assertThat(connection.get("watchKey")).isEqualTo("newValue");
		
		connection.unwatch();
	}

	@Test
	void shouldHandleHashOperations() {
		connection.hSet("hash", "field1", "value1");
		connection.hSet("hash", "field2", "value2");
		
		assertThat(connection.hGet("hash", "field1")).isEqualTo("value1");
		assertThat(connection.hGet("hash", "field2")).isEqualTo("value2");
		assertThat(connection.hLen("hash")).isEqualTo(2L);
	}

	@Test
	void shouldHandleListOperations() {
		connection.lPush("list", "value1");
		connection.lPush("list", "value2");
		connection.rPush("list", "value3");
		
		assertThat(connection.lLen("list")).isEqualTo(3L);
		assertThat(connection.lPop("list")).isEqualTo("value2");
		assertThat(connection.rPop("list")).isEqualTo("value3");
	}

	@Test
	void shouldHandleSetOperations() {
		connection.sAdd("set", "member1");
		connection.sAdd("set", "member2");
		connection.sAdd("set", "member3");
		
		assertThat(connection.sCard("set")).isEqualTo(3L);
		assertThat(connection.sIsMember("set", "member1")).isTrue();
		assertThat(connection.sIsMember("set", "member4")).isFalse();
	}
}

