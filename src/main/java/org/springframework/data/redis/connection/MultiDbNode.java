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

import org.jspecify.annotations.Nullable;
import org.springframework.util.Assert;

/**
 * A single weighted database endpoint of a {@link RedisMultiDbConfiguration multi-database} client-side geo-failover
 * setup. Extends {@link RedisNode} (mirroring {@code RedisClusterNode extends RedisNode}) and adds the multi-database
 * specific properties: a {@code weight}, an optional per-node {@code database} index and optional per-node credentials.
 * <p>
 * Per-node credentials are supported because both drivers accept per-database authentication natively, and active-active
 * geo deployments commonly use distinct credentials per region. When {@code null}, credentials are inherited from the
 * parent {@link RedisMultiDbConfiguration}.
 * <p>
 * Database selection is intentionally an advanced, per-endpoint override only: it is discouraged in modern Redis (and
 * unsupported on Redis Cluster), so it is not exposed on the parent {@link RedisMultiDbConfiguration}. Nodes that do not
 * set it use {@link #DEFAULT_DATABASE}.
 *
 * @author Tihomir Mateev
 * @since 4.0
 * @see RedisMultiDbConfiguration
 */
public class MultiDbNode extends RedisNode {

	/** Default weight applied when no explicit weight is set. */
	public static final float DEFAULT_WEIGHT = 1.0f;

	/** Default database index applied when no explicit per-node database is set. */
	public static final int DEFAULT_DATABASE = 0;

	private @Nullable Float weight;
	private @Nullable Integer database;
	private @Nullable String username;
	private @Nullable RedisPassword password;

	/**
	 * Create a new {@link MultiDbNode} with the given {@code host} and {@code port}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port a valid TCP port.
	 */
	public MultiDbNode(String host, int port) {
		super(host, port);
	}

	/**
	 * Create a new {@link MultiDbNode} for the given {@code host} and {@code port}. Entry point for the fluent
	 * configuration API, e.g. {@code MultiDbNode.host("redis.example.com", 6379).withWeight(0.5f)}.
	 *
	 * @param host must not be {@literal null}.
	 * @param port a valid TCP port.
	 * @return a new {@link MultiDbNode}.
	 */
	public static MultiDbNode host(String host, int port) {

		Assert.notNull(host, "Host must not be null");

		return new MultiDbNode(host, port);
	}

	/**
	 * @return the explicitly configured weight or {@literal null} if not set.
	 */
	public @Nullable Float getWeight() {
		return weight;
	}

	/**
	 * @return the configured weight, or {@link #DEFAULT_WEIGHT} if none was set.
	 */
	public float getWeightOrDefault() {
		return weight != null ? weight : DEFAULT_WEIGHT;
	}

	/**
	 * Apply the given {@code weight} to this node.
	 *
	 * @param weight the relative weight of this node.
	 * @return {@code this} {@link MultiDbNode}.
	 */
	public MultiDbNode withWeight(float weight) {

		this.weight = weight;
		return this;
	}

	/**
	 * @return the explicitly configured database index or {@literal null} if not set.
	 */
	public @Nullable Integer getDatabase() {
		return database;
	}

	/**
	 * @return the configured database index, or the given {@code defaultDatabase} if none was set.
	 */
	public int getDatabaseOrDefault(int defaultDatabase) {
		return database != null ? database : defaultDatabase;
	}

	/**
	 * Apply the given {@code database} index to this node. Database selection is an advanced, per-endpoint override; when
	 * not set the node uses {@link #DEFAULT_DATABASE}.
	 *
	 * @param database a non-negative database index.
	 * @return {@code this} {@link MultiDbNode}.
	 */
	public MultiDbNode withDatabase(int database) {

		Assert.isTrue(database >= 0, "Invalid DB index '%d'; non-negative index required".formatted(database));

		this.database = database;
		return this;
	}

	/**
	 * @return the per-node username or {@literal null} to inherit from the parent configuration.
	 */
	public @Nullable String getUsername() {
		return username;
	}

	/**
	 * @return the per-node password or {@literal null} to inherit from the parent configuration.
	 */
	public @Nullable RedisPassword getPassword() {
		return password;
	}

	/**
	 * Apply per-node authentication using the given {@code username} and {@code password}. When not set, credentials are
	 * inherited from the parent {@link RedisMultiDbConfiguration}.
	 *
	 * @param username can be {@literal null}.
	 * @param password must not be {@literal null}; use {@link RedisPassword#none()} for no password.
	 * @return {@code this} {@link MultiDbNode}.
	 */
	public MultiDbNode withAuthentication(@Nullable String username, RedisPassword password) {

		Assert.notNull(password, "RedisPassword must not be null");

		this.username = username;
		this.password = password;
		return this;
	}

	/**
	 * Apply per-node authentication using the given {@code password} without a username.
	 *
	 * @param password must not be {@literal null}; use {@link RedisPassword#none()} for no password.
	 * @return {@code this} {@link MultiDbNode}.
	 */
	public MultiDbNode withAuthentication(RedisPassword password) {
		return withAuthentication(null, password);
	}
}
