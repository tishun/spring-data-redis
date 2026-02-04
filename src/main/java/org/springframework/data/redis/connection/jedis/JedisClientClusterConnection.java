/*
 * Copyright 2015-present the original author or authors.
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.*;
import redis.clients.jedis.RedisClusterClient;

import java.util.*;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.util.Assert;

/**
 * {@link RedisClusterConnection} implementation using Jedis 7.2+ {@link RedisClusterClient} API.
 * <p>
 * This implementation uses the new {@link RedisClusterClient} class introduced in Jedis 7.2.0
 * for managing Redis Cluster operations.
 * <p>
 * This class is not Thread-safe and instances should not be shared across threads.
 * <p>
 * This implementation currently supports basic cluster-specific methods only. Full Redis command
 * support is not yet implemented.
 *
 * @author Tihomir Mateev
 * @since 3.5
 * @see RedisClusterClient
 * @see JedisClusterConnection
 */
@NullUnmarked
public class JedisClientClusterConnection extends AbstractRedisConnection implements RedisClusterConnection {

	private final RedisClusterClient clusterClient;

	private final Log log = LogFactory.getLog(getClass());

	private boolean closed;

	/**
	 * Constructs a new {@link JedisClientClusterConnection} instance.
	 *
	 * @param clusterClient must not be {@literal null}.
	 */
	public JedisClientClusterConnection(RedisClusterClient clusterClient) {
		Assert.notNull(clusterClient, "RedisClusterClient must not be null");
		this.clusterClient = clusterClient;
	}

	@Override
	public String ping(@NonNull RedisClusterNode node) {
		Assert.notNull(node, "RedisClusterNode must not be null");
		// RedisClusterClient extends UnifiedJedis which has cluster command methods
		throw new UnsupportedOperationException("Cluster operations not yet implemented. Use JedisConnectionFactory for Cluster support.");
	}

	@Override
	public Set<byte @NonNull []> keys(@NonNull RedisClusterNode node, byte @NonNull [] pattern) {
		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(pattern, "Pattern must not be null");
		// RedisClusterClient extends UnifiedJedis which has cluster command methods
		throw new UnsupportedOperationException("Cluster operations not yet implemented. Use JedisConnectionFactory for Cluster support.");
	}

	@Override
	public Cursor<byte @NonNull []> scan(@NonNull RedisClusterNode node, @NonNull ScanOptions options) {
		Assert.notNull(node, "RedisClusterNode must not be null");
		Assert.notNull(options, "ScanOptions must not be null");
		// RedisClusterClient extends UnifiedJedis which has cluster command methods
		throw new UnsupportedOperationException("Cluster operations not yet implemented. Use JedisConnectionFactory for Cluster support.");
	}

	@Override
	public byte[] randomKey(@NonNull RedisClusterNode node) {
		Assert.notNull(node, "RedisClusterNode must not be null");
		// RedisClusterClient extends UnifiedJedis which has cluster command methods
		throw new UnsupportedOperationException("Cluster operations not yet implemented. Use JedisConnectionFactory for Cluster support.");
	}

	@Override
	public <T> T execute(@NonNull String command, byte @NonNull [] key, @NonNull Collection<byte @NonNull []> args) {
		Assert.notNull(command, "Command must not be null");
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(args, "Args must not be null");
		// RedisClusterClient extends UnifiedJedis which has cluster command methods
		throw new UnsupportedOperationException("Cluster operations not yet implemented. Use JedisConnectionFactory for Cluster support.");
	}

	// Command provider methods required by AbstractRedisConnection

	@Override
	public RedisCommands commands() {
		return this;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		throw new UnsupportedOperationException("Geo commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisHashCommands hashCommands() {
		throw new UnsupportedOperationException("Hash commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		throw new UnsupportedOperationException("HyperLogLog commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisKeyCommands keyCommands() {
		throw new UnsupportedOperationException("Key commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisListCommands listCommands() {
		throw new UnsupportedOperationException("List commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisSetCommands setCommands() {
		throw new UnsupportedOperationException("Set commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		throw new UnsupportedOperationException("Scripting commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisClusterCommands clusterCommands() {
		return null;
	}

	@Override
	public RedisClusterServerCommands serverCommands() {
		throw new UnsupportedOperationException("Server commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisStreamCommands streamCommands() {
		throw new UnsupportedOperationException("Stream commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisStringCommands stringCommands() {
		throw new UnsupportedOperationException("String commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		throw new UnsupportedOperationException("ZSet commands not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public Object execute(String command, byte[]... args) {
		throw new UnsupportedOperationException("Execute command not yet implemented for JedisClientClusterConnection");
	}

	// Lifecycle and connection management methods

	@Override
	public void close() throws DataAccessException {
		try {
			super.close();
			clusterClient.close();
		} catch (Exception ex) {
			log.warn("Cannot properly close cluster command executor", ex);
		}

		closed = true;
	}

	@Override
	public RedisClusterClient getNativeConnection() {
		return clusterClient;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public boolean isQueueing() {
		return false; // Transactions not yet supported
	}

	@Override
	public boolean isPipelined() {
		return false; // Pipelining not yet supported
	}

	@Override
	public void openPipeline() {
		throw new UnsupportedOperationException("Pipelining is not yet supported by JedisClientClusterConnection");
	}

	@Override
	public List<@Nullable Object> closePipeline() {
		return Collections.emptyList();
	}

	@Override
	public void select(int dbIndex) {
		// Cluster mode doesn't support database selection
	}

	@Override
	public byte[] echo(byte @NonNull [] message) {
		Assert.notNull(message, "Message must not be null");
		throw new UnsupportedOperationException("Echo command not yet implemented for JedisClientClusterConnection");
	}

	@Override
	public String ping() {
		throw new UnsupportedOperationException("Ping command not yet implemented for JedisClientClusterConnection. Use ping(RedisClusterNode) instead.");
	}

	// Transaction methods from RedisTxCommands

	@Override
	public void multi() {
		// Transactions not yet supported in cluster mode
	}

	@Override
	public List<@Nullable Object> exec() {
		return List.of();
	}

	@Override
	public void discard() {
		// Transactions not yet supported in cluster mode
	}

	@Override
	public void watch(byte @NonNull []... keys) {
		// Transactions not yet supported in cluster mode
	}

	@Override
	public void unwatch() {
		// Transactions not yet supported in cluster mode
	}

	@Override
	public Iterable<@NonNull RedisClusterNode> clusterGetNodes() {
		return null;
	}

	@Override
	public Collection<@NonNull RedisClusterNode> clusterGetReplicas(@NonNull RedisClusterNode master) {
		return List.of();
	}

	@Override
	public Map<@NonNull RedisClusterNode, @NonNull Collection<@NonNull RedisClusterNode>> clusterGetMasterReplicaMap() {
		return Map.of();
	}

	@Override
	public Integer clusterGetSlotForKey(byte @NonNull [] key) {
		return 0;
	}

	@Override
	public RedisClusterNode clusterGetNodeForSlot(int slot) {
		return null;
	}

	@Override
	public RedisClusterNode clusterGetNodeForKey(byte @NonNull [] key) {
		return null;
	}

	@Override
	public ClusterInfo clusterGetClusterInfo() {
		return null;
	}

	@Override
	public void clusterAddSlots(@NonNull RedisClusterNode node, int @NonNull ... slots) {

	}

	@Override
	public void clusterAddSlots(@NonNull RedisClusterNode node, RedisClusterNode.@NonNull SlotRange range) {

	}

	@Override
	public Long clusterCountKeysInSlot(int slot) {
		return 0L;
	}

	@Override
	public void clusterDeleteSlots(@NonNull RedisClusterNode node, int @NonNull ... slots) {

	}

	@Override
	public void clusterDeleteSlotsInRange(@NonNull RedisClusterNode node, RedisClusterNode.@NonNull SlotRange range) {

	}

	@Override
	public void clusterForget(@NonNull RedisClusterNode node) {

	}

	@Override
	public void clusterMeet(@NonNull RedisClusterNode node) {

	}

	@Override
	public void clusterSetSlot(@NonNull RedisClusterNode node, int slot, @NonNull AddSlots mode) {

	}

	@Override
	public List<byte[]> clusterGetKeysInSlot(int slot, @NonNull Integer count) {
		return List.of();
	}

	@Override
	public void clusterReplicate(@NonNull RedisClusterNode master, @NonNull RedisClusterNode replica) {

	}

	@Override
	public boolean isSubscribed() {
		return false;
	}

	@Override
	public @Nullable Subscription getSubscription() {
		return null;
	}

	@Override
	public Long publish(byte @NonNull [] channel, byte @NonNull [] message) {
		return 0L;
	}

	@Override
	public void subscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull ... channels) {

	}

	@Override
	public void pSubscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull ... patterns) {

	}
}

