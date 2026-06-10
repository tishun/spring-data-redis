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
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import org.springframework.data.redis.connection.MultiDbClientOptions.HealthCheckPolicy;
import org.springframework.data.redis.connection.MultiDbClientOptions.InitialDatabaseState;

/**
 * Unit tests for {@link MultiDbClientOptions}.
 *
 * @author Tihomir Mateev
 */
class MultiDbClientOptionsUnitTests {

	@Test // GH-3253
	void defaultsShouldMatchSpecification() {

		MultiDbClientOptions options = MultiDbClientOptions.defaults();

		assertThat(options.getFailureRateThreshold()).isEqualTo(10f);
		assertThat(options.getMinimumNumberOfFailures()).isEqualTo(1000);
		assertThat(options.getSlidingWindowSize()).isEqualTo(Duration.ofSeconds(2));
		assertThat(options.getTrackedExceptions()).containsExactly(Exception.class);

		assertThat(options.isFailbackEnabled()).isTrue();
		assertThat(options.getFailbackCheckInterval()).isEqualTo(Duration.ofSeconds(120));
		assertThat(options.getGracePeriod()).isEqualTo(Duration.ofSeconds(60));

		assertThat(options.getDelayBetweenFailoverAttempts()).isEqualTo(Duration.ofSeconds(12));

		assertThat(options.isHealthCheckEnabled()).isTrue();
		assertThat(options.getHealthCheckInterval()).isEqualTo(Duration.ofSeconds(5));
		assertThat(options.getHealthCheckTimeout()).isEqualTo(Duration.ofSeconds(3));
		assertThat(options.getHealthCheckNumberOfProbes()).isEqualTo(3);
		assertThat(options.getHealthCheckDelayBetweenProbes()).isEqualTo(Duration.ofMillis(500));
		assertThat(options.getHealthCheckPolicy()).isEqualTo(HealthCheckPolicy.ALL);

		assertThat(options.getInitialDatabaseState()).isEqualTo(InitialDatabaseState.MAJORITY_AVAILABLE);
	}

	@Test // GH-3253
	void fluentApiShouldApplyAllValues() {

		Set<Class<? extends Throwable>> tracked = Set.of(RuntimeException.class, IllegalStateException.class);

		MultiDbClientOptions options = MultiDbClientOptions.defaults() //
				.failureRateThreshold(25f) //
				.minimumNumberOfFailures(50) //
				.slidingWindowSize(Duration.ofSeconds(30)) //
				.trackedExceptions(tracked) //
				.failbackEnabled(false) //
				.failbackCheckInterval(Duration.ofMinutes(5)) //
				.gracePeriod(Duration.ofSeconds(90)) //
				.delayBetweenFailoverAttempts(Duration.ofSeconds(7)) //
				.healthCheckEnabled(false) //
				.healthCheckInterval(Duration.ofSeconds(10)) //
				.healthCheckTimeout(Duration.ofSeconds(4)) //
				.healthCheckNumberOfProbes(5) //
				.healthCheckDelayBetweenProbes(Duration.ofMillis(250)) //
				.healthCheckPolicy(HealthCheckPolicy.MAJORITY) //
				.initialDatabaseState(InitialDatabaseState.ONE_AVAILABLE);

		assertThat(options.getFailureRateThreshold()).isEqualTo(25f);
		assertThat(options.getMinimumNumberOfFailures()).isEqualTo(50);
		assertThat(options.getSlidingWindowSize()).isEqualTo(Duration.ofSeconds(30));
		assertThat(options.getTrackedExceptions()).containsExactlyInAnyOrderElementsOf(tracked);
		assertThat(options.isFailbackEnabled()).isFalse();
		assertThat(options.getFailbackCheckInterval()).isEqualTo(Duration.ofMinutes(5));
		assertThat(options.getGracePeriod()).isEqualTo(Duration.ofSeconds(90));
		assertThat(options.getDelayBetweenFailoverAttempts()).isEqualTo(Duration.ofSeconds(7));
		assertThat(options.isHealthCheckEnabled()).isFalse();
		assertThat(options.getHealthCheckInterval()).isEqualTo(Duration.ofSeconds(10));
		assertThat(options.getHealthCheckTimeout()).isEqualTo(Duration.ofSeconds(4));
		assertThat(options.getHealthCheckNumberOfProbes()).isEqualTo(5);
		assertThat(options.getHealthCheckDelayBetweenProbes()).isEqualTo(Duration.ofMillis(250));
		assertThat(options.getHealthCheckPolicy()).isEqualTo(HealthCheckPolicy.MAJORITY);
		assertThat(options.getInitialDatabaseState()).isEqualTo(InitialDatabaseState.ONE_AVAILABLE);
	}

	@Test // GH-3253
	void trackedExceptionsVarargsShouldApplyValuesInOrder() {

		MultiDbClientOptions options = MultiDbClientOptions.defaults() //
				.trackedExceptions(IOException.class, TimeoutException.class);

		assertThat(options.getTrackedExceptions()).containsExactly(IOException.class, TimeoutException.class);
	}

	@Test // GH-3253
	void trackedExceptionsShouldBeUnmodifiable() {

		MultiDbClientOptions options = MultiDbClientOptions.defaults();

		assertThatExceptionOfType(UnsupportedOperationException.class)
				.isThrownBy(() -> options.getTrackedExceptions().add(RuntimeException.class));
	}

	@Test // GH-3253
	void fluentApiShouldRejectNullDurations() {

		MultiDbClientOptions options = MultiDbClientOptions.defaults();

		assertThatIllegalArgumentException().isThrownBy(() -> options.slidingWindowSize(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.failbackCheckInterval(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.gracePeriod(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.delayBetweenFailoverAttempts(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.healthCheckInterval(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.healthCheckTimeout(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.healthCheckDelayBetweenProbes(null));
	}

	@Test // GH-3253
	void fluentApiShouldRejectNullEnumsAndCollections() {

		MultiDbClientOptions options = MultiDbClientOptions.defaults();

		assertThatIllegalArgumentException().isThrownBy(() -> options.trackedExceptions((Set<Class<? extends Throwable>>) null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.healthCheckPolicy(null));
		assertThatIllegalArgumentException().isThrownBy(() -> options.initialDatabaseState(null));
	}
}
