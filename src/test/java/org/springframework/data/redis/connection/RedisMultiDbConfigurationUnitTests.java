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

import java.io.IOException;
import java.time.Duration;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.MultiDbClientOptions.HealthCheckPolicy;
import org.springframework.data.redis.connection.MultiDbClientOptions.InitialDatabaseState;
import org.springframework.mock.env.MockPropertySource;

/**
 * Unit tests for {@link RedisMultiDbConfiguration}.
 *
 * @author Tihomir Mateev
 */
class RedisMultiDbConfigurationUnitTests {

	private static MultiDbNode node(String host, int port) {
		return MultiDbNode.host(host, port);
	}

	@Test // GH-3253
	void defaultsShouldBeEmptyConfiguration() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();

		assertThat(configuration.getNodes()).isEmpty();
		assertThat(configuration.getClientOptions()).isNotNull();
		assertThat(configuration.getUsername()).isNull();
		assertThat(configuration.getPassword()).isEqualTo(RedisPassword.none());
	}

	@Test // GH-3253
	void fluentApiShouldCollectNodesAndCredentials() {

		MultiDbClientOptions options = MultiDbClientOptions.defaults().healthCheckEnabled(false);

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.descending() //
				.node(node("primary", 6379)) //
				.node(node("secondary", 6380)) //
				.clientOptions(options) //
				.username("alice") //
				.password("secret");

		assertThat(configuration.getNodes()).extracting(RedisNode::asString) //
				.containsExactly("primary:6379", "secondary:6380");
		assertThat(configuration.getClientOptions()).isSameAs(options);
		assertThat(configuration.getUsername()).isEqualTo("alice");
		assertThat(configuration.getPassword()).isEqualTo(RedisPassword.of("secret"));
		assertThat(RedisConfiguration.isMultiDbConfiguration(configuration)).isTrue();
	}

	@Test // GH-3253
	void descendingShouldDeriveWeightsFromOrder() {

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.descending() //
				.node(node("a", 6379)) //
				.node(node("b", 6380)) //
				.node(node("c", 6381));

		assertThat(configuration.getNodes()).extracting(MultiDbNode::getWeightOrDefault) //
				.containsExactly(1.0f, 2.0f / 3, 1.0f / 3);
	}

	@Test // GH-3253
	void ascendingShouldDeriveWeightsFromOrder() {

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.ascending() //
				.node(node("a", 6379)) //
				.node(node("b", 6380)) //
				.node(node("c", 6381));

		assertThat(configuration.getNodes()).extracting(MultiDbNode::getWeightOrDefault) //
				.containsExactly(1.0f / 3, 2.0f / 3, 1.0f);
	}

	@Test // GH-3253
	void explicitWeightShouldOverrideDerivedWeight() {

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.descending() //
				.node(node("a", 6379)) //
				.node(node("b", 6380).withWeight(0.25f)) //
				.node(node("c", 6381));

		assertThat(configuration.getNodes()).extracting(MultiDbNode::getWeightOrDefault) //
				.containsExactly(1.0f, 0.25f, 1.0f / 3);
	}

	@Test // GH-3253
	void getNodesShouldReturnUnmodifiableList() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration(List.of(node("h", 6379)));

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> configuration.getNodes().add(node("x", 6380)));
	}

	@Test // GH-3253
	void addNodeShouldRejectDuplicateHostPort() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();
		configuration.addNode(node("primary", 6379));

		assertThatIllegalArgumentException().isThrownBy(() -> configuration.addNode(node("primary", 6379)))
				.withMessageContaining("Duplicate node");
	}

	@Test // GH-3253
	void addNodeShouldRejectInvalidPort() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();

		assertThatIllegalArgumentException().isThrownBy(() -> configuration.addNode(node("h", 0)))
				.withMessageContaining("port must be in range");
		assertThatIllegalArgumentException().isThrownBy(() -> configuration.addNode(node("h", 70000)))
				.withMessageContaining("port must be in range");
	}

	@Test // GH-3253
	void addNodeShouldRejectNegativeWeight() {

		MultiDbNode bad = MultiDbNode.host("h", 6379).withWeight(-0.1f);

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();

		assertThatIllegalArgumentException().isThrownBy(() -> configuration.addNode(bad))
				.withMessageContaining("weight must not be negative");
	}

	@Test // GH-3253
	void addNodeShouldRejectNullOrBlankHost() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();

		assertThatIllegalArgumentException().isThrownBy(() -> configuration.addNode(new MultiDbNode("", 6379)));
	}

	@Test // GH-3253
	void settersShouldRejectNull() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();

		assertThatIllegalArgumentException().isThrownBy(() -> configuration.setClientOptions(null));
		assertThatIllegalArgumentException().isThrownBy(() -> configuration.setPassword((RedisPassword) null));
	}

	@Test // GH-3253
	void ofShouldRejectNullPropertySource() {
		assertThatIllegalArgumentException().isThrownBy(() -> RedisMultiDbConfiguration.of(null));
	}

	@Test // GH-3253
	void ofShouldYieldEmptyConfigurationWhenSourceHasNoRelevantProperties() {

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.of(new MockPropertySource());

		assertThat(configuration.getNodes()).isEmpty();
		assertThat(configuration.getUsername()).isNull();
		assertThat(configuration.getPassword()).isEqualTo(RedisPassword.none());
		assertThat(configuration.getClientOptions()).isNotNull();
	}

	@Test // GH-3253
	void ofShouldReadNodesInPriorityOrder() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.nodes", "east.example.com:6379, west.example.com:6380");

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.of(propertySource);

		assertThat(configuration.getNodes()).extracting(RedisNode::asString) //
				.containsExactly("east.example.com:6379", "west.example.com:6380");
		assertThat(configuration.getNodes()).extracting(MultiDbNode::getWeightOrDefault) //
				.containsExactly(1.0f, 0.5f);
	}

	@Test // GH-3253
	void ofShouldReadCredentials() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.nodes", "east.example.com:6379");
		propertySource.setProperty("spring.redis.multidb.username", "alice");
		propertySource.setProperty("spring.redis.multidb.password", "secret");

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.of(propertySource);

		assertThat(configuration.getUsername()).isEqualTo("alice");
		assertThat(configuration.getPassword()).isEqualTo(RedisPassword.of("secret"));
	}

	@Test // GH-3253
	void ofShouldLeaveClientOptionDefaultsUntouchedWhenAbsent() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.nodes", "east.example.com:6379");

		MultiDbClientOptions options = RedisMultiDbConfiguration.of(propertySource).getClientOptions();

		assertThat(options.getFailureRateThreshold()).isEqualTo(MultiDbClientOptions.defaults().getFailureRateThreshold());
		assertThat(options.getSlidingWindowSize()).isEqualTo(MultiDbClientOptions.defaults().getSlidingWindowSize());
		assertThat(options.getHealthCheckPolicy()).isEqualTo(MultiDbClientOptions.defaults().getHealthCheckPolicy());
	}

	@Test // GH-3253
	void ofShouldReadAllClientOptionsWithCoercion() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.nodes", "east.example.com:6379");
		propertySource.setProperty("spring.redis.multidb.failure-rate-threshold", "25");
		propertySource.setProperty("spring.redis.multidb.minimum-number-of-failures", "500");
		propertySource.setProperty("spring.redis.multidb.sliding-window-size", "10s");
		propertySource.setProperty("spring.redis.multidb.failback-enabled", "false");
		propertySource.setProperty("spring.redis.multidb.failback-check-interval", "30s");
		propertySource.setProperty("spring.redis.multidb.grace-period", "1m");
		propertySource.setProperty("spring.redis.multidb.delay-between-failover-attempts", "5s");
		propertySource.setProperty("spring.redis.multidb.health-check-enabled", "false");
		propertySource.setProperty("spring.redis.multidb.health-check-interval", "7s");
		propertySource.setProperty("spring.redis.multidb.health-check-timeout", "2s");
		propertySource.setProperty("spring.redis.multidb.health-check-number-of-probes", "5");
		propertySource.setProperty("spring.redis.multidb.health-check-delay-between-probes", "250ms");
		propertySource.setProperty("spring.redis.multidb.health-check-policy", "majority");
		propertySource.setProperty("spring.redis.multidb.initial-database-state", "ALL_AVAILABLE");

		MultiDbClientOptions options = RedisMultiDbConfiguration.of(propertySource).getClientOptions();

		assertThat(options.getFailureRateThreshold()).isEqualTo(25f);
		assertThat(options.getMinimumNumberOfFailures()).isEqualTo(500);
		assertThat(options.getSlidingWindowSize()).isEqualTo(Duration.ofSeconds(10));
		assertThat(options.isFailbackEnabled()).isFalse();
		assertThat(options.getFailbackCheckInterval()).isEqualTo(Duration.ofSeconds(30));
		assertThat(options.getGracePeriod()).isEqualTo(Duration.ofMinutes(1));
		assertThat(options.getDelayBetweenFailoverAttempts()).isEqualTo(Duration.ofSeconds(5));
		assertThat(options.isHealthCheckEnabled()).isFalse();
		assertThat(options.getHealthCheckInterval()).isEqualTo(Duration.ofSeconds(7));
		assertThat(options.getHealthCheckTimeout()).isEqualTo(Duration.ofSeconds(2));
		assertThat(options.getHealthCheckNumberOfProbes()).isEqualTo(5);
		assertThat(options.getHealthCheckDelayBetweenProbes()).isEqualTo(Duration.ofMillis(250));
		assertThat(options.getHealthCheckPolicy()).isEqualTo(HealthCheckPolicy.MAJORITY);
		assertThat(options.getInitialDatabaseState()).isEqualTo(InitialDatabaseState.ALL_AVAILABLE);
	}

	@Test // GH-3253
	void ofShouldParseTrackedExceptions() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.nodes", "east.example.com:6379");
		propertySource.setProperty("spring.redis.multidb.tracked-exceptions",
				"java.io.IOException, java.lang.RuntimeException");

		MultiDbClientOptions options = RedisMultiDbConfiguration.of(propertySource).getClientOptions();

		assertThat(options.getTrackedExceptions()).containsExactly(IOException.class, RuntimeException.class);
	}

	@Test // GH-3253
	void ofShouldRejectUnknownTrackedException() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.tracked-exceptions", "com.example.DoesNotExist");

		assertThatIllegalArgumentException().isThrownBy(() -> RedisMultiDbConfiguration.of(propertySource))
				.withMessageContaining("Invalid tracked exception class");
	}

	@Test // GH-3253
	void ofShouldRejectNonThrowableTrackedException() {

		MockPropertySource propertySource = new MockPropertySource();
		propertySource.setProperty("spring.redis.multidb.tracked-exceptions", "java.lang.String");

		assertThatIllegalArgumentException().isThrownBy(() -> RedisMultiDbConfiguration.of(propertySource))
				.withMessageContaining("must be a subtype of Throwable");
	}
}
