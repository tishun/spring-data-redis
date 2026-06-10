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

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.util.Assert;

/**
 * Cross-driver options for client-side geographic failover (multi-database) clients. Carries the subset of failover
 * settings exposed by both Jedis ({@code MultiDbConfig}) and Lettuce ({@code MultiDbOptions}) and normalized to Spring
 * Data Redis conventions (JavaBean accessors, {@link Duration} time values, full-word property names).
 * <p>
 * Instances are created pre-populated with the defaults recommended by the client-side geo-failover design specification
 * via {@link #defaults()} and configured fluently, e.g.
 * {@code MultiDbClientOptions.defaults().failureRateThreshold(25f).healthCheckEnabled(false)}.
 * <p>
 * Driver-specific knobs that have no counterpart on the other driver (for example Jedis {@code commandRetry} or
 * {@code maxNumFailoverAttempts}) are intentionally not modeled here; reach them through the driver-specific customizer
 * hooks instead.
 *
 * @author Tihomir Mateev
 * @since 4.0
 * @see RedisMultiDbConfiguration
 */
public class MultiDbClientOptions {

	/**
	 * Policy controlling when a health check counts as successful based on the configured number of probes.
	 */
	public enum HealthCheckPolicy {

		/** All probes must report healthy. */
		ALL,

		/** The majority of probes must report healthy. */
		MAJORITY,

		/** At least one probe must report healthy. */
		ANY
	}

	/**
	 * Policy controlling how many databases must be available for the client to complete initialization.
	 */
	public enum InitialDatabaseState {

		/** All databases must be available on initialization. */
		ALL_AVAILABLE,

		/** The majority of databases must be available on initialization. */
		MAJORITY_AVAILABLE,

		/** At least one database must be available on initialization. */
		ONE_AVAILABLE
	}

	private float failureRateThreshold = 10f;
	private int minimumNumberOfFailures = 1000;
	private Duration slidingWindowSize = Duration.ofSeconds(2);
	private Set<Class<? extends Throwable>> trackedExceptions = new LinkedHashSet<>(Set.of(Exception.class));

	private boolean failbackEnabled = true;
	private Duration failbackCheckInterval = Duration.ofSeconds(120);
	private Duration gracePeriod = Duration.ofSeconds(60);

	private Duration delayBetweenFailoverAttempts = Duration.ofSeconds(12);

	private boolean healthCheckEnabled = true;
	private Duration healthCheckInterval = Duration.ofSeconds(5);
	private Duration healthCheckTimeout = Duration.ofSeconds(3);
	private int healthCheckNumberOfProbes = 3;
	private Duration healthCheckDelayBetweenProbes = Duration.ofMillis(500);
	private HealthCheckPolicy healthCheckPolicy = HealthCheckPolicy.ALL;

	private InitialDatabaseState initialDatabaseState = InitialDatabaseState.MAJORITY_AVAILABLE;

	MultiDbClientOptions() {}

	/**
	 * Create a new {@link MultiDbClientOptions} instance pre-populated with the default values. Entry point for the
	 * fluent configuration API, e.g. {@code MultiDbClientOptions.defaults().failureRateThreshold(25f)}.
	 *
	 * @return a new {@link MultiDbClientOptions} with defaults applied.
	 */
	public static MultiDbClientOptions defaults() {
		return new MultiDbClientOptions();
	}

	public float getFailureRateThreshold() {
		return failureRateThreshold;
	}

	public int getMinimumNumberOfFailures() {
		return minimumNumberOfFailures;
	}

	public Duration getSlidingWindowSize() {
		return slidingWindowSize;
	}

	public Set<Class<? extends Throwable>> getTrackedExceptions() {
		return Collections.unmodifiableSet(trackedExceptions);
	}

	public boolean isFailbackEnabled() {
		return failbackEnabled;
	}

	public Duration getFailbackCheckInterval() {
		return failbackCheckInterval;
	}

	public Duration getGracePeriod() {
		return gracePeriod;
	}

	public Duration getDelayBetweenFailoverAttempts() {
		return delayBetweenFailoverAttempts;
	}

	public boolean isHealthCheckEnabled() {
		return healthCheckEnabled;
	}

	public Duration getHealthCheckInterval() {
		return healthCheckInterval;
	}

	public Duration getHealthCheckTimeout() {
		return healthCheckTimeout;
	}

	public int getHealthCheckNumberOfProbes() {
		return healthCheckNumberOfProbes;
	}

	public Duration getHealthCheckDelayBetweenProbes() {
		return healthCheckDelayBetweenProbes;
	}

	public HealthCheckPolicy getHealthCheckPolicy() {
		return healthCheckPolicy;
	}

	public InitialDatabaseState getInitialDatabaseState() {
		return initialDatabaseState;
	}

	/**
	 * Set the failure rate threshold (in percent) above which a database circuit is opened.
	 *
	 * @param failureRateThreshold the failure rate threshold in percent.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions failureRateThreshold(float failureRateThreshold) {
		this.failureRateThreshold = failureRateThreshold;
		return this;
	}

	/**
	 * Set the minimum number of failures that must be observed before the failure rate is evaluated.
	 *
	 * @param minimumNumberOfFailures the minimum number of failures.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions minimumNumberOfFailures(int minimumNumberOfFailures) {
		this.minimumNumberOfFailures = minimumNumberOfFailures;
		return this;
	}

	/**
	 * Set the sliding window duration over which failures are counted.
	 *
	 * @param slidingWindowSize must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions slidingWindowSize(Duration slidingWindowSize) {

		Assert.notNull(slidingWindowSize, "Sliding window size must not be null");

		this.slidingWindowSize = slidingWindowSize;
		return this;
	}

	/**
	 * Set the exceptions counted as failures by the failure detector, e.g.
	 * {@code trackedExceptions(IOException.class, TimeoutException.class)}.
	 *
	 * @param trackedExceptions must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	@SafeVarargs
	public final MultiDbClientOptions trackedExceptions(Class<? extends Throwable>... trackedExceptions) {

		Assert.notNull(trackedExceptions, "Tracked exceptions must not be null");

		Set<Class<? extends Throwable>> exceptions = new LinkedHashSet<>();
		Collections.addAll(exceptions, trackedExceptions);
		this.trackedExceptions = exceptions;
		return this;
	}

	/**
	 * Set the exceptions counted as failures by the failure detector.
	 *
	 * @param trackedExceptions must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions trackedExceptions(Set<Class<? extends Throwable>> trackedExceptions) {

		Assert.notNull(trackedExceptions, "Tracked exceptions must not be null");

		this.trackedExceptions = new LinkedHashSet<>(trackedExceptions);
		return this;
	}

	/**
	 * Enable or disable automatic failback to a higher-weight database once it becomes healthy again.
	 *
	 * @param failbackEnabled whether automatic failback is enabled.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions failbackEnabled(boolean failbackEnabled) {
		this.failbackEnabled = failbackEnabled;
		return this;
	}

	/**
	 * Set the interval at which the client checks whether a higher-weight database is available for failback.
	 *
	 * @param failbackCheckInterval must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions failbackCheckInterval(Duration failbackCheckInterval) {

		Assert.notNull(failbackCheckInterval, "Failback check interval must not be null");

		this.failbackCheckInterval = failbackCheckInterval;
		return this;
	}

	/**
	 * Set the grace period for which an opened circuit stays open before transitioning to half-open.
	 *
	 * @param gracePeriod must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions gracePeriod(Duration gracePeriod) {

		Assert.notNull(gracePeriod, "Grace period must not be null");

		this.gracePeriod = gracePeriod;
		return this;
	}

	/**
	 * Set the delay between successive failover attempts.
	 *
	 * @param delayBetweenFailoverAttempts must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions delayBetweenFailoverAttempts(Duration delayBetweenFailoverAttempts) {

		Assert.notNull(delayBetweenFailoverAttempts, "Delay between failover attempts must not be null");

		this.delayBetweenFailoverAttempts = delayBetweenFailoverAttempts;
		return this;
	}

	/**
	 * Enable or disable health checking of databases.
	 *
	 * @param healthCheckEnabled whether health checking is enabled.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions healthCheckEnabled(boolean healthCheckEnabled) {
		this.healthCheckEnabled = healthCheckEnabled;
		return this;
	}

	/**
	 * Set the interval between health checks.
	 *
	 * @param healthCheckInterval must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions healthCheckInterval(Duration healthCheckInterval) {

		Assert.notNull(healthCheckInterval, "Health check interval must not be null");

		this.healthCheckInterval = healthCheckInterval;
		return this;
	}

	/**
	 * Set the overall timeout for a single health check operation.
	 *
	 * @param healthCheckTimeout must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions healthCheckTimeout(Duration healthCheckTimeout) {

		Assert.notNull(healthCheckTimeout, "Health check timeout must not be null");

		this.healthCheckTimeout = healthCheckTimeout;
		return this;
	}

	/**
	 * Set the number of probes performed per health check.
	 *
	 * @param healthCheckNumberOfProbes the number of probes.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions healthCheckNumberOfProbes(int healthCheckNumberOfProbes) {
		this.healthCheckNumberOfProbes = healthCheckNumberOfProbes;
		return this;
	}

	/**
	 * Set the delay between probes within a single health check.
	 *
	 * @param healthCheckDelayBetweenProbes must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions healthCheckDelayBetweenProbes(Duration healthCheckDelayBetweenProbes) {

		Assert.notNull(healthCheckDelayBetweenProbes, "Health check delay between probes must not be null");

		this.healthCheckDelayBetweenProbes = healthCheckDelayBetweenProbes;
		return this;
	}

	/**
	 * Set the policy determining when probes count a database as healthy.
	 *
	 * @param healthCheckPolicy must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions healthCheckPolicy(HealthCheckPolicy healthCheckPolicy) {

		Assert.notNull(healthCheckPolicy, "Health check policy must not be null");

		this.healthCheckPolicy = healthCheckPolicy;
		return this;
	}

	/**
	 * Set the policy determining how many databases must be available on initialization.
	 *
	 * @param initialDatabaseState must not be {@literal null}.
	 * @return {@code this} {@link MultiDbClientOptions}.
	 */
	public MultiDbClientOptions initialDatabaseState(InitialDatabaseState initialDatabaseState) {

		Assert.notNull(initialDatabaseState, "Initial database state must not be null");

		this.initialDatabaseState = initialDatabaseState;
		return this;
	}
}
