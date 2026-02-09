/*
 * Copyright 2026-present the original author or authors.
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
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.redis.connection.*;
import org.springframework.data.redis.connection.convert.TransactionResultConverter;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.Protocol;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.redis.ExceptionTranslationStrategy;
import org.springframework.data.redis.FallbackExceptionTranslationStrategy;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisResultBuilder;
import org.springframework.data.redis.connection.jedis.JedisResult.JedisStatusResult;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * {@code RedisConnection} implementation on top of <a href="https://github.com/redis/jedis">Jedis</a> 7.2+ library
 * using the {@link RedisClient} API.
 * <p>
 * This class is not Thread-safe and instances should not be shared across threads.
 * <p>
 * This implementation reuses the existing command classes from the legacy {@link JedisConnection} implementation,
 * providing full support for all Redis commands, pipelining, transactions, and pub/sub.
 * <p>
 * Note: {@link RedisClient} extends {@code UnifiedJedis}, which allows it to be safely cast to {@link Jedis}
 * for command execution, enabling code reuse with the existing command infrastructure.
 *
 * @author Tihomir Mateev
 * @since 3.5
 * @see RedisClient
 * @see JedisConnection
 */
@NullUnmarked
public class JedisClientConnection extends AbstractRedisConnection {

	private static final ExceptionTranslationStrategy EXCEPTION_TRANSLATION = new FallbackExceptionTranslationStrategy(
			JedisExceptionConverter.INSTANCE);

	private boolean convertPipelineAndTxResults = true;

	private final RedisClient redisClient;

	private final JedisClientConfig clientConfig;

	private volatile @Nullable JedisSubscription subscription;

	private final JedisClientGeoCommands geoCommands = new JedisClientGeoCommands(this);
	private final JedisClientHashCommands hashCommands = new JedisClientHashCommands(this);
	private final JedisClientHyperLogLogCommands hllCommands = new JedisClientHyperLogLogCommands(this);
	private final JedisClientKeyCommands keyCommands = new JedisClientKeyCommands(this);
	private final JedisClientListCommands listCommands = new JedisClientListCommands(this);
	private final JedisClientScriptingCommands scriptingCommands = new JedisClientScriptingCommands(this);
	private final JedisClientServerCommands serverCommands = new JedisClientServerCommands(this);
	private final JedisClientSetCommands setCommands = new JedisClientSetCommands(this);
	private final JedisClientStreamCommands streamCommands = new JedisClientStreamCommands(this);
	private final JedisClientStringCommands stringCommands = new JedisClientStringCommands(this);
	private final JedisClientZSetCommands zSetCommands = new JedisClientZSetCommands(this);

	private final Log log = LogFactory.getLog(getClass());

	@SuppressWarnings("rawtypes") private List<JedisResult> pipelinedResults = new ArrayList<>();

	private Queue<FutureResult<Response<?>>> txResults = new LinkedList<>();

	private volatile @Nullable Pipeline pipeline;

	private volatile @Nullable AbstractTransaction transaction;

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
	 * Execute a Redis command with identity conversion (no transformation).
	 *
	 * @param directFunction function to execute in direct mode on RedisClient
	 * @param pipelineFunction function to execute in pipelined/transactional mode on PipeliningBase
	 * @param <T> the result type
	 * @return the command result, or null in pipelined/transactional mode
	 */
	<T> @Nullable T execute(Function<RedisClient, T> directFunction,
			Function<PipeliningBase, Response<T>> pipelineFunction) {

		return doWithRedisClient(client -> {

			if (isQueueing()) {
				Response<T> response = pipelineFunction.apply(getRequiredTransaction());
				transaction(newJedisResult(response));
				return null;
			}

			if (isPipelined()) {
				Response<T> response = pipelineFunction.apply(getRequiredPipeline());
				pipeline(newJedisResult(response));
				return null;
			}

			// Direct execution
			return directFunction.apply(client);
		});
	}

	/**
	 * Execute a Redis command that returns a status response.
	 * Status responses are handled specially and not included in transactional results.
	 *
	 * @param directFunction function to execute in direct mode on RedisClient
	 * @param pipelineFunction function to execute in pipelined/transactional mode on PipeliningBase
	 * @param <T> the result type
	 * @return the command result, or null in pipelined/transactional mode
	 */
	<T> @Nullable T executeStatus(Function<RedisClient, T> directFunction,
			Function<PipeliningBase, Response<T>> pipelineFunction) {

		return doWithRedisClient(client -> {

			if (isQueueing()) {
				Response<T> response = pipelineFunction.apply(getRequiredTransaction());
				transaction(newStatusResult(response));
				return null;
			}

			if (isPipelined()) {
				Response<T> response = pipelineFunction.apply(getRequiredPipeline());
				pipeline(newStatusResult(response));
				return null;
			}

			// Direct execution
			return directFunction.apply(client);
		});
	}

	/**
	 * Execute a command with a custom converter.
	 *
	 * @param directFunction function to execute in direct mode on RedisClient
	 * @param pipelineFunction function to execute in pipelined/transactional mode on PipeliningBase
	 * @param converter converter to transform the result
	 * @param <S> the source type
	 * @param <T> the target type
	 * @return the converted command result, or null in pipelined/transactional mode
	 */
	<S, T> @Nullable T execute(Function<RedisClient, S> directFunction,
			Function<PipeliningBase, Response<S>> pipelineFunction, Converter<S, T> converter) {

		return doWithRedisClient(client -> {

			if (isQueueing()) {
				Response<S> response = pipelineFunction.apply(getRequiredTransaction());
				transaction(newJedisResult(response, converter, () -> null));
				return null;
			}

			if (isPipelined()) {
				Response<S> response = pipelineFunction.apply(getRequiredPipeline());
				pipeline(newJedisResult(response, converter, () -> null));
				return null;
			}

			// Direct execution
			S result = directFunction.apply(client);
			return result != null ? converter.convert(result) : null;
		});
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
		return geoCommands;
	}

	@Override
	public RedisHashCommands hashCommands() {
		return hashCommands;
	}

	@Override
	public RedisHyperLogLogCommands hyperLogLogCommands() {
		return hllCommands;
	}

	@Override
	public RedisKeyCommands keyCommands() {
		return keyCommands;
	}

	@Override
	public RedisListCommands listCommands() {
		return listCommands;
	}

	@Override
	public RedisSetCommands setCommands() {
		return setCommands;
	}

	@Override
	public RedisScriptingCommands scriptingCommands() {
		return scriptingCommands;
	}

	@Override
	public RedisServerCommands serverCommands() {
		return serverCommands;
	}

	@Override
	public RedisStreamCommands streamCommands() {
		return streamCommands;
	}

	@Override
	public RedisStringCommands stringCommands() {
		return stringCommands;
	}

	@Override
	public RedisZSetCommands zSetCommands() {
		return zSetCommands;
	}

	@Override
	public Object execute(@NonNull String command, byte @NonNull []... args) {

		Assert.hasText(command, "A valid command needs to be specified");
		Assert.notNull(args, "Arguments must not be null");

		return doWithRedisClient(client -> {

			ProtocolCommand protocolCommand = () -> JedisConverters.toBytes(command);

			if (isQueueing() || isPipelined()) {

				CommandArguments arguments = new CommandArguments(protocolCommand).addObjects(args);
				CommandObject<Object> commandObject = new CommandObject<>(arguments, BuilderFactory.RAW_OBJECT);

				if (isPipelined()) {
					pipeline(newJedisResult(getRequiredPipeline().executeCommand(commandObject)));
				} else {
					transaction(newJedisResult(getRequiredTransaction().executeCommand(commandObject)));
				}
				return null;
			}

			return client.sendCommand(protocolCommand, args);
		});
	}

	@Override
	public void close() throws DataAccessException {

		super.close();

		JedisSubscription subscription = this.subscription;

		if (subscription != null) {
			doExceptionThrowingOperationSafely(subscription::close, "Cannot terminate subscription");
			this.subscription = null;
		}

		// RedisClient is managed by the factory, so we don't close it here
	}

	@Override
	public RedisClient getNativeConnection() {
		return this.redisClient;
	}

	@Override
	public boolean isClosed() {
		// RedisClient doesn't expose connection state directly
		// We rely on the factory to manage the lifecycle
		return false;
	}

	@Override
	public boolean isQueueing() {
		return this.transaction != null;
	}

	@Override
	public boolean isPipelined() {
		return this.pipeline != null;
	}

	@Override
	public void openPipeline() {

		if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Pipelining while a transaction is active");
		}

		if (pipeline == null) {
			pipeline = redisClient.pipelined();
		}
	}

	@Override
	public List<@Nullable Object> closePipeline() {

		if (pipeline != null) {
			try {
				return convertPipelineResults();
			} finally {
				pipeline = null;
				pipelinedResults.clear();
			}
		}

		return Collections.emptyList();
	}

	private List<@Nullable Object> convertPipelineResults() {

		List<Object> results = new ArrayList<>();

		getRequiredPipeline().sync();

		Exception cause = null;

		for (JedisResult<?, ?> result : pipelinedResults) {
			try {

				Object data = result.get();

				if (!result.isStatus()) {
					results.add(result.conversionRequired() ? result.convert(data) : data);
				}
			} catch (Exception ex) {
				DataAccessException dataAccessException = convertJedisAccessException(ex);
				if (cause == null) {
					cause = dataAccessException;
				}
				results.add(dataAccessException);
			}
		}

		if (cause != null) {
			throw new RedisPipelineException(cause, results);
		}

		return results;
	}

	void pipeline(@NonNull JedisResult<?, ?> result) {

		if (isQueueing()) {
			transaction(result);
		} else {
			pipelinedResults.add(result);
		}
	}

	void transaction(@NonNull FutureResult<Response<?>> result) {
		txResults.add(result);
	}

	@Override
	public void select(int dbIndex) {
		doWithRedisClient((Consumer<RedisClient>) client -> client.sendCommand(Protocol.Command.SELECT, String.valueOf(dbIndex)));
	}

	@Override
	public byte[] echo(byte @NonNull [] message) {

		Assert.notNull(message, "Message must not be null");

		return doWithRedisClient(client -> (byte[]) client.sendCommand(Protocol.Command.ECHO, message));
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

	public @Nullable Pipeline getPipeline() {
		return this.pipeline;
	}

	public Pipeline getRequiredPipeline() {

		Pipeline pipeline = getPipeline();

		Assert.state(pipeline != null, "Connection has no active pipeline");

		return pipeline;
	}

	public @Nullable AbstractTransaction getTransaction() {
		return this.transaction;
	}

	public AbstractTransaction getRequiredTransaction() {

		AbstractTransaction transaction = getTransaction();

		Assert.state(transaction != null, "Connection has no active transaction");

		return transaction;
	}

	/**
	 * Returns the underlying {@link RedisClient} instance.
	 * <p>
	 * Note: RedisClient extends UnifiedJedis, so it can be safely cast to Jedis for command execution.
	 *
	 * @return the {@link RedisClient} instance. Never {@literal null}.
	 */
	@NonNull
	public RedisClient getRedisClient() {
		return this.redisClient;
	}

	/**
	 * Returns the underlying {@link RedisClient} instance.
	 * <p>
	 * Note: {@link RedisClient} extends {@link UnifiedJedis}, not {@link Jedis}.
	 * This method is used by SCAN operations in command classes.
	 *
	 * @return the {@link RedisClient}. Never {@literal null}.
	 */
	@NonNull
	public RedisClient getJedis() {
		return this.redisClient;
	}



	<T> JedisResult<T, T> newJedisResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response)
				.convertPipelineAndTxResults(convertPipelineAndTxResults)
				.build();
	}

	<T, R> JedisResult<T, R> newJedisResult(Response<T> response, Converter<T, R> converter, Supplier<R> defaultValue) {

		return JedisResultBuilder.<T, R> forResponse(response).mappedWith(converter)
				.convertPipelineAndTxResults(convertPipelineAndTxResults).mapNullTo(defaultValue).build();
	}

	<T> JedisStatusResult<T, T> newStatusResult(Response<T> response) {
		return JedisResultBuilder.<T, T> forResponse(response).buildStatusResult();
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

	private void doExceptionThrowingOperationSafely(Runnable operation, String errorMessage) {
		try {
			operation.run();
		} catch (Exception ex) {
			log.warn(errorMessage, ex);
		}
	}

	//
	// Pub/Sub functionality
	//

	@Override
	public Long publish(byte @NonNull [] channel, byte @NonNull [] message) {
		return doWithRedisClient((Function<RedisClient, Long>) client -> client.publish(channel, message));
	}

	@Override
	public Subscription getSubscription() {
		return subscription;
	}

	@Override
	public boolean isSubscribed() {
		return subscription != null && subscription.isAlive();
	}

	@Override
	public void subscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull ... channels) {

		if (isSubscribed()) {
			throw new InvalidDataAccessApiUsageException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, channels, null);
			redisClient.subscribe(jedisPubSub, channels);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	@Override
	public void pSubscribe(@NonNull MessageListener listener, byte @NonNull [] @NonNull ... patterns) {

		if (isSubscribed()) {
			throw new InvalidDataAccessApiUsageException(
					"Connection already subscribed; use the connection Subscription to cancel or add new channels");
		}

		try {
			BinaryJedisPubSub jedisPubSub = new JedisMessageListener(listener);
			subscription = new JedisSubscription(listener, jedisPubSub, null, patterns);
			redisClient.psubscribe(jedisPubSub, patterns);
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		}
	}

	//
	// Transaction functionality
	//

	@Override
	public void multi() {

		if (isQueueing()) {
			return;
		}

		if (isPipelined()) {
			throw new InvalidDataAccessApiUsageException("Cannot use Transaction while a pipeline is open");
		}

		doWithRedisClient(client -> {
			this.transaction = client.multi();
		});
	}

	@Override
	public List<@Nullable Object> exec() {

		try {

			if (transaction == null) {
				throw new InvalidDataAccessApiUsageException("No ongoing transaction; Did you forget to call multi");
			}

			List<Object> results = transaction.exec();

			return !CollectionUtils.isEmpty(results)
					? new TransactionResultConverter<>(txResults, JedisExceptionConverter.INSTANCE).convert(results)
					: results;

		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	@Override
	public void discard() {

		try {
			getRequiredTransaction().discard();
		} catch (Exception ex) {
			throw convertJedisAccessException(ex);
		} finally {
			txResults.clear();
			transaction = null;
		}
	}

	@Override
	public void watch(byte @NonNull [] @NonNull... keys) {

		if (isQueueing()) {
			throw new InvalidDataAccessApiUsageException("WATCH is not supported when a transaction is active");
		}

		doWithRedisClient((Consumer<RedisClient>) client -> client.sendCommand(Protocol.Command.WATCH, keys));
	}

	@Override
	public void unwatch() {
		doWithRedisClient((Consumer<RedisClient>) client -> client.sendCommand(Protocol.Command.UNWATCH));
	}
}

