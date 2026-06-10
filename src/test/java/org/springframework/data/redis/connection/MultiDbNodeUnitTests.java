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
package org.springframework.data.redis.connection;

import static org.assertj.core.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MultiDbNode}.
 *
 * @author Tihomir Mateev
 */
class MultiDbNodeUnitTests {

	@Test // GH-3253
	void constructorShouldSetHostAndPort() {

		MultiDbNode node = new MultiDbNode("redis.example.com", 6380);

		assertThat(node.getHost()).isEqualTo("redis.example.com");
		assertThat(node.getPort()).isEqualTo(6380);
		assertThat(node.getWeight()).isNull();
		assertThat(node.getWeightOrDefault()).isEqualTo(MultiDbNode.DEFAULT_WEIGHT);
		assertThat(node.getDatabase()).isNull();
		assertThat(node.getDatabaseOrDefault(MultiDbNode.DEFAULT_DATABASE)).isEqualTo(MultiDbNode.DEFAULT_DATABASE);
		assertThat(node.getUsername()).isNull();
		assertThat(node.getPassword()).isNull();
	}

	@Test // GH-3253
	void fluentApiShouldApplyAllValues() {

		RedisPassword password = RedisPassword.of("secret");

		MultiDbNode node = MultiDbNode.host("primary.example.com", 6379) //
				.withWeight(0.75f) //
				.withAuthentication("alice", password);

		assertThat(node.getHost()).isEqualTo("primary.example.com");
		assertThat(node.getPort()).isEqualTo(6379);
		assertThat(node.getWeight()).isEqualTo(0.75f);
		assertThat(node.getWeightOrDefault()).isEqualTo(0.75f);
		assertThat(node.getUsername()).isEqualTo("alice");
		assertThat(node.getPassword()).isEqualTo(password);
	}

	@Test // GH-3253
	void hostShouldApplyHostAndPort() {

		MultiDbNode node = MultiDbNode.host("replica.example.com", 6381);

		assertThat(node.getHost()).isEqualTo("replica.example.com");
		assertThat(node.getPort()).isEqualTo(6381);
		assertThat(node.getWeight()).isNull();
		assertThat(node.getUsername()).isNull();
		assertThat(node.getPassword()).isNull();
	}

	@Test // GH-3253
	void hostShouldRejectNullHost() {
		assertThatIllegalArgumentException().isThrownBy(() -> MultiDbNode.host(null, 6379));
	}

	@Test // GH-3253
	void weightOrDefaultShouldReturnExplicitWeight() {

		MultiDbNode node = MultiDbNode.host("h", 6379).withWeight(2.5f);

		assertThat(node.getWeightOrDefault()).isEqualTo(2.5f);
	}

	@Test // GH-3253
	void withAuthenticationShouldApplyPasswordOnly() {

		MultiDbNode node = MultiDbNode.host("h", 6379).withAuthentication(RedisPassword.of("pw"));

		assertThat(node.getUsername()).isNull();
		assertThat(node.getPassword()).isEqualTo(RedisPassword.of("pw"));
	}

	@Test // GH-3253
	void withAuthenticationShouldRejectNullPassword() {

		MultiDbNode node = MultiDbNode.host("h", 6379);

		assertThatIllegalArgumentException().isThrownBy(() -> node.withAuthentication("u", null));
	}

	@Test // GH-3253
	void withDatabaseShouldApplyIndex() {

		MultiDbNode node = MultiDbNode.host("h", 6379).withDatabase(3);

		assertThat(node.getDatabase()).isEqualTo(3);
		assertThat(node.getDatabaseOrDefault(MultiDbNode.DEFAULT_DATABASE)).isEqualTo(3);
	}

	@Test // GH-3253
	void withDatabaseShouldRejectNegativeIndex() {
		assertThatIllegalArgumentException().isThrownBy(() -> MultiDbNode.host("h", 6379).withDatabase(-1));
	}
}
