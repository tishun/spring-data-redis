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
package org.springframework.data.redis.connection.jedis;

import org.springframework.data.redis.connection.MultiDbNode;

import redis.clients.jedis.MultiDbConfig;

/**
 * Strategy interface for customizing the Jedis {@link MultiDbConfig.DatabaseConfig.Builder} for an individual
 * {@link MultiDbNode database endpoint} of a client-side geographic failover (multi-database) setup. Spring populates
 * the builder with the endpoint, the resolved {@link redis.clients.jedis.JedisClientConfig}, weight and pool
 * configuration and then invokes the customizer last, so implementations only have to apply per-endpoint deltas (for
 * example a different {@code healthCheckStrategySupplier} / {@code LagAwareStrategy} or pool sizing for the far region).
 * <p>
 * The {@link MultiDbNode} is supplied so per-endpoint customizations can be addressed by node.
 *
 * @author Tihomir Mateev
 * @since 4.0
 * @see redis.clients.jedis.MultiDbConfig.DatabaseConfig.Builder
 */
@FunctionalInterface
public interface DatabaseConfigBuilderCustomizer {

	/**
	 * Customize the {@link MultiDbConfig.DatabaseConfig.Builder} for the given {@link MultiDbNode}.
	 *
	 * @param node the database endpoint the builder is being created for.
	 * @param builder the builder to customize.
	 */
	void customize(MultiDbNode node, MultiDbConfig.DatabaseConfig.Builder builder);

}
