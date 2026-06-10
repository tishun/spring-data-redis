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
package org.springframework.data.redis.connection.lettuce;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.failover.MultiDbClient;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

import java.util.concurrent.CompletionStage;

import org.springframework.util.Assert;

/**
 * {@link LettuceConnectionProvider} implementation for a Lettuce {@link MultiDbClient client-side geographic failover
 * (multi-database)} setup. Returns {@link io.lettuce.core.failover.api.StatefulRedisMultiDbConnection multi-database
 * connections} that transparently fail over between the configured database endpoints.
 *
 * @author Tihomir Mateev
 * @since 4.0
 */
class MultiDbConnectionProvider implements LettuceConnectionProvider, RedisClientProvider {

	private final MultiDbClient client;
	private final RedisCodec<?, ?> codec;

	/**
	 * Create a new {@link MultiDbConnectionProvider}.
	 *
	 * @param client must not be {@literal null}.
	 * @param codec must not be {@literal null}.
	 */
	MultiDbConnectionProvider(MultiDbClient client, RedisCodec<?, ?> codec) {

		Assert.notNull(client, "MultiDbClient must not be null");
		Assert.notNull(codec, "RedisCodec must not be null");

		this.client = client;
		this.codec = codec;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T extends StatefulConnection<?, ?>> T getConnection(Class<T> connectionType) {

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return connectionType.cast(client.connectPubSub((RedisCodec) codec));
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {
			return connectionType.cast(client.connect((RedisCodec) codec));
		}

		throw new UnsupportedOperationException("Connection type " + connectionType + " not supported");
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T extends StatefulConnection<?, ?>> CompletionStage<T> getConnectionAsync(Class<T> connectionType) {

		if (connectionType.equals(StatefulRedisPubSubConnection.class)) {
			return client.connectPubSubAsync((RedisCodec) codec).thenApply(connectionType::cast);
		}

		if (StatefulConnection.class.isAssignableFrom(connectionType)) {
			return client.connectAsync((RedisCodec) codec).thenApply(connectionType::cast);
		}

		return LettuceFutureUtils
				.failed(new UnsupportedOperationException("Connection type " + connectionType + " not supported"));
	}

	@Override
	public AbstractRedisClient getRedisClient() {
		return (AbstractRedisClient) client;
	}

	/**
	 * @return the underlying {@link MultiDbClient}.
	 */
	MultiDbClient getMultiDbClient() {
		return client;
	}
}
