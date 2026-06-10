/*
 * Copyright 2011-present the original author or authors.
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
package org.springframework.data.redis;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.springframework.data.redis.connection.MultiDbNode;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.RedisMultiDbConfiguration;
import org.springframework.data.redis.connection.RedisPassword;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisSocketConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;

/**
 * Utility class exposing connection settings to connect Redis instances during test execution. Settings can be adjusted
 * by overriding these in {@literal org/springframework/data/redis/test.properties}.
 *
 * @author Costin Leau
 * @author Mark Paluch
 * @author John Blum
 */
public abstract class SettingsUtils {

	private static final Properties DEFAULTS = new Properties();
	private static final Properties SETTINGS;

	static {
		DEFAULTS.put("host", "127.0.0.1");
		DEFAULTS.put("port", "6379");
		DEFAULTS.put("clusterPort", "7379");
		DEFAULTS.put("sentinelPort", "26379");
		DEFAULTS.put("socket", "work/redis-6379.sock");

		SETTINGS = new Properties(DEFAULTS);

		try {
			SETTINGS.load(SettingsUtils.class.getResourceAsStream("/org/springframework/data/redis/test.properties"));
		} catch (Exception ignore) {
			throw new IllegalArgumentException("Cannot read settings");
		}
	}

	private SettingsUtils() {}

	/**
	 * @return the Redis hostname.
	 */
	public static String getHost() {
		return SETTINGS.getProperty("host");
	}

	/**
	 * @return the Redis port.
	 */
	public static int getPort() {
		return Integer.parseInt(SETTINGS.getProperty("port"));
	}

	/**
	 * @return the Redis Cluster port.
	 */
	public static int getSentinelPort() {
		return Integer.parseInt(SETTINGS.getProperty("sentinelPort"));
	}

	/**
	 * @return the Redis Sentinel Master Id.
	 */
	public static String getSentinelMaster() {
		return "mymaster";
	}

	/**
	 * @return the Redis Cluster port.
	 */
	public static int getClusterPort() {
		return Integer.parseInt(SETTINGS.getProperty("clusterPort"));
	}

	/**
	 * @return path to the unix domain socket.
	 */
	public static String getSocket() {
		return SETTINGS.getProperty("socket");
	}

	/**
	 * Construct a new {@link RedisStandaloneConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisStandaloneConfiguration} initialized with test endpoint settings.
	 */
	public static RedisStandaloneConfiguration standaloneConfiguration() {
		return new RedisStandaloneConfiguration(getHost(), getPort());
	}

	/**
	 * Construct a new {@link RedisSentinelConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisSentinelConfiguration} initialized with test endpoint settings.
	 */
	public static RedisSentinelConfiguration sentinelConfiguration() {

		List<String> sentinelHostPorts = List.of("%s:%d".formatted(getHost(), getSentinelPort()),
				"%s:%d".formatted(getHost(), getSentinelPort() + 1));

		return new RedisSentinelConfiguration(getSentinelMaster(), new HashSet<>(sentinelHostPorts));
	}

	/**
	 * Construct a new {@link RedisClusterConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisClusterConfiguration} initialized with test endpoint settings.
	 */
	public static RedisClusterConfiguration clusterConfiguration() {
		return new RedisClusterConfiguration(List.of("%s:%d".formatted(getHost(), getClusterPort())));
	}

	/**
	 * Construct a new {@link RedisSocketConfiguration} initialized with test endpoint settings.
	 *
	 * @return a new {@link RedisSocketConfiguration} initialized with test endpoint settings.
	 */
	public static RedisSocketConfiguration socketConfiguration() {
		return new RedisSocketConfiguration(getSocket());
	}

	/**
	 * @return the highest-weight endpoint port for the Mode M1 multi-database setup. Reuses the shared no-auth standalone
	 *         node so no dedicated infrastructure is required.
	 */
	public static int getMultiDbPortA() {
		return 6379;
	}

	/**
	 * @return the lower-weight endpoint port for the Mode M1 multi-database setup. Reuses the shared auth-protected
	 *         standalone node (password {@code foobared}) as an independent second region.
	 */
	public static int getMultiDbPortB() {
		return 6382;
	}

	/**
	 * Construct a new {@link RedisMultiDbConfiguration} for Mode M1 — two independent standalone Redis instances acting
	 * as stand-ins for separate geographic regions. The two nodes share no data; failover discards writes made against
	 * the previous endpoint, matching realistic cache / session / rate-limiter workloads. The setup reuses the shared
	 * no-auth node (6379) and auth-protected node (6382, password {@code foobared} supplied per-node) rather than
	 * dedicated containers.
	 *
	 * @return a new {@link RedisMultiDbConfiguration} initialized with test endpoint settings.
	 */
	public static RedisMultiDbConfiguration multiDbConfiguration() {

		return RedisMultiDbConfiguration.descending()
				.node(MultiDbNode.host(getHost(), getMultiDbPortA()))
				.node(MultiDbNode.host(getHost(), getMultiDbPortB())
						.withAuthentication(RedisPassword.of("foobared")));
	}

	/**
	 * Construct a new {@link RedisMultiDbConfiguration} for Mode M2 — geo-distributed OSS read-replicas of the existing
	 * Sentinel master. Targets the two replica ports; writes must be routed via a separate {@code RedisConnectionFactory}
	 * aimed at the master.
	 *
	 * @return a new {@link RedisMultiDbConfiguration} initialized with replica endpoint settings.
	 */
	public static RedisMultiDbConfiguration multiDbReplicaConfiguration() {

		return RedisMultiDbConfiguration.descending()
				.node(MultiDbNode.host(getHost(), 6380))
				.node(MultiDbNode.host(getHost(), 6381));
	}

}
