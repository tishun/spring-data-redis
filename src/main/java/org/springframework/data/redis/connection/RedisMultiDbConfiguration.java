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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.core.env.PropertySource;
import org.springframework.data.redis.connection.MultiDbClientOptions.HealthCheckPolicy;
import org.springframework.data.redis.connection.MultiDbClientOptions.InitialDatabaseState;
import org.springframework.data.redis.connection.RedisConfiguration.WithPassword;
import org.springframework.format.datetime.standard.DurationFormatterUtils;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

/**
 * Configuration class used to set up a client-side geographic failover (multi-database) {@link RedisConnection} via
 * {@link RedisConnectionFactory}. Carries an ordered list of weighted {@link MultiDbNode database endpoints} plus the
 * cross-driver {@link MultiDbClientOptions failover options} shared by Jedis and Lettuce.
 * <p>
 * When supplied to a connection factory, the factory builds the driver's multi-database client instead of a regular
 * single-endpoint client. The configuration is optional and additive; existing configuration types are unaffected.
 *
 * @author Tihomir Mateev
 * @since 4.0
 * @see MultiDbNode
 * @see MultiDbClientOptions
 */
public class RedisMultiDbConfiguration implements RedisConfiguration, WithPassword {

	private static final String REDIS_MULTIDB_NODES_CONFIG_PROPERTY = "spring.redis.multidb.nodes";
	private static final String REDIS_MULTIDB_USERNAME_CONFIG_PROPERTY = "spring.redis.multidb.username";
	private static final String REDIS_MULTIDB_PASSWORD_CONFIG_PROPERTY = "spring.redis.multidb.password";
	private static final String REDIS_MULTIDB_FAILURE_RATE_THRESHOLD_CONFIG_PROPERTY = "spring.redis.multidb.failure-rate-threshold";
	private static final String REDIS_MULTIDB_MINIMUM_NUMBER_OF_FAILURES_CONFIG_PROPERTY = "spring.redis.multidb.minimum-number-of-failures";
	private static final String REDIS_MULTIDB_SLIDING_WINDOW_SIZE_CONFIG_PROPERTY = "spring.redis.multidb.sliding-window-size";
	private static final String REDIS_MULTIDB_TRACKED_EXCEPTIONS_CONFIG_PROPERTY = "spring.redis.multidb.tracked-exceptions";
	private static final String REDIS_MULTIDB_FAILBACK_ENABLED_CONFIG_PROPERTY = "spring.redis.multidb.failback-enabled";
	private static final String REDIS_MULTIDB_FAILBACK_CHECK_INTERVAL_CONFIG_PROPERTY = "spring.redis.multidb.failback-check-interval";
	private static final String REDIS_MULTIDB_GRACE_PERIOD_CONFIG_PROPERTY = "spring.redis.multidb.grace-period";
	private static final String REDIS_MULTIDB_DELAY_BETWEEN_FAILOVER_ATTEMPTS_CONFIG_PROPERTY = "spring.redis.multidb.delay-between-failover-attempts";
	private static final String REDIS_MULTIDB_HEALTH_CHECK_ENABLED_CONFIG_PROPERTY = "spring.redis.multidb.health-check-enabled";
	private static final String REDIS_MULTIDB_HEALTH_CHECK_INTERVAL_CONFIG_PROPERTY = "spring.redis.multidb.health-check-interval";
	private static final String REDIS_MULTIDB_HEALTH_CHECK_TIMEOUT_CONFIG_PROPERTY = "spring.redis.multidb.health-check-timeout";
	private static final String REDIS_MULTIDB_HEALTH_CHECK_NUMBER_OF_PROBES_CONFIG_PROPERTY = "spring.redis.multidb.health-check-number-of-probes";
	private static final String REDIS_MULTIDB_HEALTH_CHECK_DELAY_BETWEEN_PROBES_CONFIG_PROPERTY = "spring.redis.multidb.health-check-delay-between-probes";
	private static final String REDIS_MULTIDB_HEALTH_CHECK_POLICY_CONFIG_PROPERTY = "spring.redis.multidb.health-check-policy";
	private static final String REDIS_MULTIDB_INITIAL_DATABASE_STATE_CONFIG_PROPERTY = "spring.redis.multidb.initial-database-state";

	private final List<MultiDbNode> nodes = new ArrayList<>();
	private final List<Boolean> weightExplicit = new ArrayList<>();
	private @Nullable WeightStrategy weightStrategy;
	private MultiDbClientOptions clientOptions = MultiDbClientOptions.defaults();
	private @Nullable String username = null;
	private RedisPassword password = RedisPassword.none();

	/**
	 * Create a new, empty {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration() {}

	/**
	 * Create a new {@link RedisMultiDbConfiguration} for the given {@link MultiDbNode nodes}.
	 *
	 * @param nodes must not be {@literal null} or empty.
	 */
	public RedisMultiDbConfiguration(List<MultiDbNode> nodes) {

		Assert.notNull(nodes, "Nodes must not be null");

		nodes.forEach(this::addNode);
	}

	/**
	 * Entry point for the fluent configuration API that derives node weights in <em>descending</em> order of
	 * registration: the first {@link MultiDbNode node} added receives the highest weight (preferred for active and
	 * failback), each subsequent node a lower weight. Nodes that declare an explicit {@link MultiDbNode#withWeight(float)
	 * weight} keep it. Example:
	 * {@code RedisMultiDbConfiguration.descending().node(primary).node(secondary)}.
	 *
	 * @return a new {@link RedisMultiDbConfiguration}.
	 */
	public static RedisMultiDbConfiguration descending() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();
		configuration.weightStrategy = WeightStrategy.DESCENDING;
		return configuration;
	}

	/**
	 * Entry point for the fluent configuration API that derives node weights in <em>ascending</em> order of
	 * registration: the first {@link MultiDbNode node} added receives the lowest weight, the last node the highest
	 * (preferred for active and failback). Nodes that declare an explicit {@link MultiDbNode#withWeight(float) weight}
	 * keep it. Example: {@code RedisMultiDbConfiguration.ascending().node(secondary).node(primary)}.
	 *
	 * @return a new {@link RedisMultiDbConfiguration}.
	 */
	public static RedisMultiDbConfiguration ascending() {

		RedisMultiDbConfiguration configuration = new RedisMultiDbConfiguration();
		configuration.weightStrategy = WeightStrategy.ASCENDING;
		return configuration;
	}

	/**
	 * Construct a new {@link RedisMultiDbConfiguration} from the given {@link PropertySource}, mirroring
	 * {@link RedisClusterConfiguration#of(PropertySource)} / {@link RedisSentinelConfiguration#of(PropertySource)}. Nodes
	 * are read from {@code spring.redis.multidb.nodes} (comma-delimited {@code host:port}, order = priority via
	 * {@link #descending()}); the parent credentials and the cross-driver {@link MultiDbClientOptions} are read from the
	 * remaining {@code spring.redis.multidb.*} keys. Every key is optional except {@code nodes}; absent keys leave the
	 * {@link MultiDbClientOptions#defaults() default} value untouched. Per-node weights, per-node credentials and per-node
	 * database overrides are intentionally not exposed here and remain available through the programmatic DSL.
	 *
	 * @param propertySource must not be {@literal null}.
	 * @return a new {@link RedisMultiDbConfiguration} configured from the given {@link PropertySource}.
	 */
	public static RedisMultiDbConfiguration of(PropertySource<?> propertySource) {

		Assert.notNull(propertySource, "PropertySource must not be null");

		RedisMultiDbConfiguration configuration = RedisMultiDbConfiguration.descending();

		String nodes = getProperty(propertySource, REDIS_MULTIDB_NODES_CONFIG_PROPERTY);
		if (nodes != null) {
			for (String hostAndPort : StringUtils.commaDelimitedListToStringArray(nodes)) {

				if (!StringUtils.hasText(hostAndPort)) {
					continue;
				}

				RedisNode parsed = RedisNode.fromString(hostAndPort.trim());
				configuration.node(MultiDbNode.host(parsed.getRequiredHost(), parsed.getRequiredPort()));
			}
		}

		String username = getProperty(propertySource, REDIS_MULTIDB_USERNAME_CONFIG_PROPERTY);
		if (username != null) {
			configuration.setUsername(username);
		}

		String password = getProperty(propertySource, REDIS_MULTIDB_PASSWORD_CONFIG_PROPERTY);
		if (password != null) {
			configuration.password(password);
		}

		applyClientOptions(propertySource, configuration.getClientOptions());

		return configuration;
	}

	private static void applyClientOptions(PropertySource<?> propertySource, MultiDbClientOptions options) {

		String failureRateThreshold = getProperty(propertySource, REDIS_MULTIDB_FAILURE_RATE_THRESHOLD_CONFIG_PROPERTY);
		if (failureRateThreshold != null) {
			options.failureRateThreshold(Float.parseFloat(failureRateThreshold.trim()));
		}

		String minimumNumberOfFailures = getProperty(propertySource, REDIS_MULTIDB_MINIMUM_NUMBER_OF_FAILURES_CONFIG_PROPERTY);
		if (minimumNumberOfFailures != null) {
			options.minimumNumberOfFailures(parseInt(minimumNumberOfFailures));
		}

		String slidingWindowSize = getProperty(propertySource, REDIS_MULTIDB_SLIDING_WINDOW_SIZE_CONFIG_PROPERTY);
		if (slidingWindowSize != null) {
			options.slidingWindowSize(parseDuration(slidingWindowSize));
		}

		String trackedExceptions = getProperty(propertySource, REDIS_MULTIDB_TRACKED_EXCEPTIONS_CONFIG_PROPERTY);
		if (trackedExceptions != null) {
			options.trackedExceptions(parseTrackedExceptions(trackedExceptions));
		}

		String failbackEnabled = getProperty(propertySource, REDIS_MULTIDB_FAILBACK_ENABLED_CONFIG_PROPERTY);
		if (failbackEnabled != null) {
			options.failbackEnabled(Boolean.parseBoolean(failbackEnabled.trim()));
		}

		String failbackCheckInterval = getProperty(propertySource, REDIS_MULTIDB_FAILBACK_CHECK_INTERVAL_CONFIG_PROPERTY);
		if (failbackCheckInterval != null) {
			options.failbackCheckInterval(parseDuration(failbackCheckInterval));
		}

		String gracePeriod = getProperty(propertySource, REDIS_MULTIDB_GRACE_PERIOD_CONFIG_PROPERTY);
		if (gracePeriod != null) {
			options.gracePeriod(parseDuration(gracePeriod));
		}

		String delayBetweenFailoverAttempts = getProperty(propertySource,
				REDIS_MULTIDB_DELAY_BETWEEN_FAILOVER_ATTEMPTS_CONFIG_PROPERTY);
		if (delayBetweenFailoverAttempts != null) {
			options.delayBetweenFailoverAttempts(parseDuration(delayBetweenFailoverAttempts));
		}

		String healthCheckEnabled = getProperty(propertySource, REDIS_MULTIDB_HEALTH_CHECK_ENABLED_CONFIG_PROPERTY);
		if (healthCheckEnabled != null) {
			options.healthCheckEnabled(Boolean.parseBoolean(healthCheckEnabled.trim()));
		}

		String healthCheckInterval = getProperty(propertySource, REDIS_MULTIDB_HEALTH_CHECK_INTERVAL_CONFIG_PROPERTY);
		if (healthCheckInterval != null) {
			options.healthCheckInterval(parseDuration(healthCheckInterval));
		}

		String healthCheckTimeout = getProperty(propertySource, REDIS_MULTIDB_HEALTH_CHECK_TIMEOUT_CONFIG_PROPERTY);
		if (healthCheckTimeout != null) {
			options.healthCheckTimeout(parseDuration(healthCheckTimeout));
		}

		String healthCheckNumberOfProbes = getProperty(propertySource,
				REDIS_MULTIDB_HEALTH_CHECK_NUMBER_OF_PROBES_CONFIG_PROPERTY);
		if (healthCheckNumberOfProbes != null) {
			options.healthCheckNumberOfProbes(parseInt(healthCheckNumberOfProbes));
		}

		String healthCheckDelayBetweenProbes = getProperty(propertySource,
				REDIS_MULTIDB_HEALTH_CHECK_DELAY_BETWEEN_PROBES_CONFIG_PROPERTY);
		if (healthCheckDelayBetweenProbes != null) {
			options.healthCheckDelayBetweenProbes(parseDuration(healthCheckDelayBetweenProbes));
		}

		String healthCheckPolicy = getProperty(propertySource, REDIS_MULTIDB_HEALTH_CHECK_POLICY_CONFIG_PROPERTY);
		if (healthCheckPolicy != null) {
			options.healthCheckPolicy(HealthCheckPolicy.valueOf(healthCheckPolicy.trim().toUpperCase(Locale.ROOT)));
		}

		String initialDatabaseState = getProperty(propertySource, REDIS_MULTIDB_INITIAL_DATABASE_STATE_CONFIG_PROPERTY);
		if (initialDatabaseState != null) {
			options.initialDatabaseState(InitialDatabaseState.valueOf(initialDatabaseState.trim().toUpperCase(Locale.ROOT)));
		}
	}

	private static @Nullable String getProperty(PropertySource<?> propertySource, String key) {

		if (!propertySource.containsProperty(key)) {
			return null;
		}

		Object value = propertySource.getProperty(key);
		return value != null ? String.valueOf(value) : null;
	}

	private static int parseInt(String value) {

		try {
			return Integer.parseInt(value.trim());
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("Invalid integer value '%s'".formatted(value), ex);
		}
	}

	private static Duration parseDuration(String value) {
		return DurationFormatterUtils.detectAndParse(value.trim());
	}

	@SuppressWarnings("unchecked")
	private static Set<Class<? extends Throwable>> parseTrackedExceptions(String value) {

		Set<Class<? extends Throwable>> exceptions = new LinkedHashSet<>();

		for (String className : StringUtils.commaDelimitedListToStringArray(value)) {

			if (!StringUtils.hasText(className)) {
				continue;
			}

			String trimmed = className.trim();

			try {
				Class<?> type = ClassUtils.forName(trimmed, RedisMultiDbConfiguration.class.getClassLoader());
				Assert.isAssignable(Throwable.class, type,
						"Tracked exception '%s' must be a subtype of Throwable".formatted(trimmed));
				exceptions.add((Class<? extends Throwable>) type);
			} catch (ClassNotFoundException ex) {
				throw new IllegalArgumentException("Invalid tracked exception class '%s'".formatted(trimmed), ex);
			}
		}

		return exceptions;
	}

	/**
	 * Add a {@link MultiDbNode database endpoint}. Alias for {@link #addNode(MultiDbNode)} that reads fluently after
	 * {@link #descending()} / {@link #ascending()}.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@code this} {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration node(MultiDbNode node) {
		return addNode(node);
	}

	/**
	 * Add a {@link MultiDbNode} to the list of database endpoints. When a weight strategy was selected via
	 * {@link #descending()} / {@link #ascending()}, weights are (re)derived from the registration order for all nodes
	 * that did not declare an explicit weight.
	 *
	 * @param node must not be {@literal null}.
	 * @return {@code this} {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration addNode(MultiDbNode node) {

		validateNode(node);
		assertNoDuplicate(node);

		this.nodes.add(node);
		this.weightExplicit.add(node.getWeight() != null);
		reapplyDerivedWeights();
		return this;
	}

	/**
	 * @return an unmodifiable {@link List} of the configured {@link MultiDbNode nodes}.
	 */
	public List<MultiDbNode> getNodes() {
		return Collections.unmodifiableList(this.nodes);
	}

	/**
	 * @return the cross-driver {@link MultiDbClientOptions}. Never {@literal null}.
	 */
	public MultiDbClientOptions getClientOptions() {
		return this.clientOptions;
	}

	/**
	 * Set the cross-driver {@link MultiDbClientOptions}.
	 *
	 * @param clientOptions must not be {@literal null}.
	 */
	public void setClientOptions(MultiDbClientOptions clientOptions) {

		Assert.notNull(clientOptions, "MultiDbClientOptions must not be null");

		this.clientOptions = clientOptions;
	}

	@Override
	public @Nullable String getUsername() {
		return this.username;
	}

	@Override
	public void setUsername(@Nullable String username) {
		this.username = username;
	}

	@Override
	public RedisPassword getPassword() {
		return this.password;
	}

	@Override
	public void setPassword(RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null");

		this.password = password;
	}

	/**
	 * Set the cross-driver {@link MultiDbClientOptions}.
	 *
	 * @param clientOptions must not be {@literal null}.
	 * @return {@code this} {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration clientOptions(MultiDbClientOptions clientOptions) {

		setClientOptions(clientOptions);
		return this;
	}

	/**
	 * Set the username inherited by nodes that do not define their own.
	 *
	 * @param username can be {@literal null}.
	 * @return {@code this} {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration username(@Nullable String username) {

		setUsername(username);
		return this;
	}

	/**
	 * Set the password inherited by nodes that do not define their own.
	 *
	 * @param password must not be {@literal null}; use {@link RedisPassword#none()} instead.
	 * @return {@code this} {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration password(RedisPassword password) {

		setPassword(password);
		return this;
	}

	/**
	 * Set the password inherited by nodes that do not define their own.
	 *
	 * @param password can be {@literal null}.
	 * @return {@code this} {@link RedisMultiDbConfiguration}.
	 */
	public RedisMultiDbConfiguration password(@Nullable String password) {
		return password(RedisPassword.of(password));
	}

	private void reapplyDerivedWeights() {

		if (this.weightStrategy == null) {
			return;
		}

		int size = this.nodes.size();
		for (int i = 0; i < size; i++) {

			if (Boolean.TRUE.equals(this.weightExplicit.get(i))) {
				continue;
			}

			float weight = this.weightStrategy == WeightStrategy.DESCENDING //
					? (float) (size - i) / size //
					: (float) (i + 1) / size;
			this.nodes.get(i).withWeight(weight);
		}
	}

	private void assertNoDuplicate(MultiDbNode node) {

		String endpoint = node.asString();

		for (MultiDbNode existing : this.nodes) {
			if (existing.asString().equals(endpoint)) {
				throw new IllegalArgumentException("Duplicate node '%s'; host:port must be unique".formatted(endpoint));
			}
		}
	}

	static void validateNode(MultiDbNode node) {

		Assert.notNull(node, "MultiDbNode must not be null");
		Assert.hasText(node.getHost(), "Node host must not be null or empty");

		int port = node.getPort();
		Assert.isTrue(port >= 1 && port <= 65535, "Invalid port '%d'; port must be in range [1, 65535]".formatted(port));

		Float weight = node.getWeight();
		if (weight != null) {
			Assert.isTrue(weight >= 0, "Invalid weight '%s'; weight must not be negative".formatted(weight));
		}
	}

	/**
	 * Strategy describing how node weights are derived from the registration order when no explicit weight is set.
	 */
	private enum WeightStrategy {

		/** First node added receives the highest weight, each subsequent node a lower one. */
		DESCENDING,

		/** First node added receives the lowest weight, the last node the highest. */
		ASCENDING
	}
}
