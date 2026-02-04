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
package org.springframework.data.redis.connection.jedis;

import org.jspecify.annotations.NullUnmarked;
import org.springframework.data.redis.connection.*;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.JedisClientConfig;
import redis.clients.jedis.RedisClient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.util.Assert;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/redis/jedis">Jedis</a> 7.2+ library
 * using the {@link RedisClient} API.
 * <p>
 * This class is not Thread-safe and instances should not be shared across threads.
 * <p>
 * This implementation currently supports basic Redis commands only. Pipeline and transaction support is not yet
 * implemented.
 *
 * @author Tihomir Mateev
 * @since 3.5
 * @see redis.clients.jedis.RedisClient
 */
@NullUnmarked
public class JedisClientConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private boolean convertPipelineAndTxResults = true;

	private final RedisClient redisClient;

	private final JedisClientConfig clientConfig;

	private final Log log = LogFactory.getLog(getClass());

	public JedisClientConnection(@NonNull RedisClient redisClient) {
		this(redisClient, DefaultJedisClientConfig.builder().build());
	}

	public JedisClientConnection(@NonNull RedisClient redisClient, int dbIndex) {
		this(redisClient, dbIndex, null);
	}

	public JedisClientConnection(@NonNull RedisClient redisClient, int dbIndex, @Nullable String clientName) {
		this(redisClient, createConfig(dbIndex, clientName));
	}

	public JedisClientConnection(@NonNull RedisClient redisClient, @NonNull JedisClientConfig clientConfig) {

		Assert.notNull(redisClient, "RedisClient must not be null");
		Assert.notNull(clientConfig, "JedisClientConfig must not be null");

		this.redisClient = redisClient;
		this.clientConfig = clientConfig;
	}

		private static DefaultJedisClientConfig createConfig(int dbIndex, @Nullable String clientName) {
		return DefaultJedisClientConfig.builder().database(dbIndex).clientName(clientName).build();
	}

	/**
	 * Converts Jedis exceptions to Spring's {@link DataAccessException} hierarchy.
	 *
	 * @param cause the exception to convert
	 * @return the converted {@link DataAccessException}
	 */
	protected DataAccessException convertJedisAccessException(Exception cause) {
		DataAccessException exception = EXCEPTION_TRANSLATION.translate(cause);
		return exception != null ? exception : new RedisSystemException(cause.getMessage(), cause);
	}

	@Override
	public RedisCommands commands() {
		return this;
	}

	@Override
	public RedisGeoCommands geoCommands() {
		throw new UnsupportedOperationException("Geo commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisHashCommands hashCommands() {
		throw new UnsupportedOperationException("Hash commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		throw new UnsupportedOperationException("HyperLogLog commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisKeyCommands keyCommands() {
		throw new UnsupportedOperationException("Key commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisListCommands listCommands() {
		throw new UnsupportedOperationException("List commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisSetCommands setCommands() {
		throw new UnsupportedOperationException("Set commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		throw new UnsupportedOperationException("Scripting commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisServerCommands serverCommands() {
		throw new UnsupportedOperationException("Server commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisStreamCommands streamCommands() {
		throw new UnsupportedOperationException("Stream commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisStringCommands stringCommands() {
		throw new UnsupportedOperationException("String commands not yet implemented for JedisClientConnection");
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		throw new UnsupportedOperationException("ZSet commands not yet implemented for JedisClientConnection");
	}

	@Override
	public Object execute(String command, byte[]... args) {
		throw new UnsupportedOperationException("Execute command not yet implemented for JedisClientConnection");
	}

	@Override
	public void close() throws DataAccessException {
		super.close();
		// RedisClient is managed by the factory, so we don't close it here
	}

	@Override
	public RedisClient getNativeConnection() {
		return this.redisClient;
	}

	@Override
	public boolean isClosed() {
		// RedisClient doesn't have an isConnected method, so we assume it's not closed
		// unless close() has been called
		return false;
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
		throw new UnsupportedOperationException("Pipelining is not yet supported by JedisClientConnection");
	}

	@Override
	public List<@Nullable Object> closePipeline() {
		return Collections.emptyList();
	}

	@Override
	public void select(int dbIndex) {

	}

	@Override
	public byte[] echo(byte @NonNull [] message) {
		Assert.notNull(message, "Message must not be null");

		return doWithRedisClient(client -> {
			return client.echo(new String(message, StandardCharsets.UTF_8));
		}).getBytes(StandardCharsets.UTF_8);
	}

	@Override
	public String ping() {
		return doWithRedisClient(RedisClient::ping);
	}

	/**
	 * Specifies if pipelined results should be converted to the expected data type.
	 *
	 * @param convertPipelineAndTxResults {@code true} to convert pipeline and transaction results.
	 */
	public void setConvertPipelineAndTxResults(boolean convertPipelineAndTxResults) {
		this.convertPipelineAndTxResults = convertPipelineAndTxResults;
	}

	/**
	 * Returns the underlying {@link RedisClient} instance.
	 *
	 * @return the {@link RedisClient} instance. Never {@literal null}.
	 */
	@NonNull
	public RedisClient getRedisClient() {
		return this.redisClient;
	}

	@Override
	protected boolean isActive(@NonNull RedisNode node) {
		// Sentinel support not yet implemented
		return false;
	}

	@Override
	protected RedisSentinelConnection getSentinelConnection(@NonNull RedisNode sentinel) {
		throw new UnsupportedOperationException("Sentinel is not supported by JedisClientConnection");
	}

	private @Nullable <T> T doWithRedisClient(@NonNull Function<@NonNull RedisClient, T> callback) {

		try {
			return callback.apply(getRedisClient());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	private void doWithRedisClient(@NonNull Consumer<@NonNull RedisClient> callback) {

		try {
			callback.accept(getRedisClient());
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
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

	@Override
	public void multi() {

	}

	@Override
	public List<@Nullable Object> exec() {
		return List.of();
	}

	@Override
	public void discard() {

	}

	@Override
	public void watch(byte @NonNull []... keys) {

	}

	@Override
	public void unwatch() {

	}
}

