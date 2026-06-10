/**
 * Integration test showcases for client-side geographic failover (multi-database) support in Spring Data Redis.
 *
 * <h2>Scope and intent</h2>
 *
 * Spring Data Redis is an <em>upstream</em> consumer of Jedis ({@code MultiDbClient}) and Lettuce
 * ({@code MultiDbConfiguration}). The drivers own circuit-breaker mechanics, retry/backoff timings, health-check
 * strategy internals, per-endpoint pooling, weight selection, per-node authentication wire-up, and driver-specific
 * exception types. The driver test suites cover those edge cases; this package does not re-assert them.
 * <p>
 * This package covers only what is unique to Spring Data Redis:
 * <ol>
 * <li><b>E2E showcase</b> &mdash; readable test methods that double as documentation. Each scenario should be
 * copy-pasteable into a user's project.</li>
 * <li><b>SDR abstraction transparency</b> &mdash; {@link org.springframework.data.redis.core.RedisTemplate
 * RedisTemplate}, {@link org.springframework.data.redis.core.StringRedisTemplate StringRedisTemplate},
 * {@link org.springframework.data.redis.core.ReactiveRedisTemplate ReactiveRedisTemplate},
 * {@link org.springframework.data.redis.listener.RedisMessageListenerContainer RedisMessageListenerContainer}, and
 * {@link org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer
 * ReactiveRedisMessageListenerContainer} continue to work as a user expects while the underlying client switches
 * endpoints.</li>
 * <li><b>Two deployment modes</b> &mdash; both real-world OSS topologies (see below).</li>
 * <li><b>Lifecycle hygiene</b> &mdash; factory {@code afterPropertiesSet} / {@code destroy} over a multi-DB
 * configuration leaves no leaked threads or resources.</li>
 * </ol>
 * Driver wiring correctness (setting propagation, customizer invocation, configuration validation) lives in the Phase D
 * unit test suite. OSS Redis only.
 *
 * <h2>Deployment modes under test</h2>
 *
 * The multi-DB client is topology-agnostic &mdash; it routes traffic to weighted endpoints and has no notion of
 * replication. OSS users land in one of two real-world modes; the showcase covers both:
 * <ul>
 * <li><b>Mode M1 &mdash; Independent standalones.</b> Reuses the shared standalone nodes {@code 6379} (no-auth, weight
 * 1.0) and {@code 6382} (auth-protected, password {@code foobared}, weight 0.5) as stand-ins for separate regions; no
 * dedicated infrastructure is required. Each endpoint has its own dataset; failover discards the previous endpoint's
 * writes. Acceptable for caches, session stores, rate-limiters, feature flags, idempotent counters. Exercised by Groups
 * S1, S2, S3, S5.</li>
 * <li><b>Mode M2 &mdash; Geo-distributed OSS read-replicas.</b> Reuses the existing master ({@code 6379}) plus
 * replicas ({@code 6380} weight 1.0, {@code 6381} weight 0.5). Same logical dataset on every endpoint (modulo
 * replication lag); writes rejected with {@code READONLY}. The multi-DB factory is the read-side; writes route via a
 * separate {@link org.springframework.data.redis.connection.RedisConnectionFactory RedisConnectionFactory} aimed at the
 * master. Acceptable for read-scaling, geo-local reads, BI / analytics / cache-warmers. M2 is the OSS-only stand-in for
 * Enterprise Active-Active. Exercised by Group S4.</li>
 * </ul>
 *
 * <h2>Infrastructure</h2>
 * <ul>
 * <li>No dedicated containers or docker-runtime manipulation. M1 reuses the shared standalone nodes ({@code 6379},
 * {@code 6382}); M2 reuses the existing Sentinel master ({@code 6379}) and replicas ({@code 6380}, {@code 6381}).</li>
 * <li>{@link org.springframework.data.redis.SettingsUtils#multiDbConfiguration() SettingsUtils.multiDbConfiguration()}
 * builds the M1 {@link org.springframework.data.redis.connection.RedisMultiDbConfiguration RedisMultiDbConfiguration};
 * {@link org.springframework.data.redis.SettingsUtils#multiDbReplicaConfiguration()
 * SettingsUtils.multiDbReplicaConfiguration()} builds the M2 variant.</li>
 * <li>{@link org.springframework.data.redis.test.condition.EnabledOnMultiDbAvailable @EnabledOnMultiDbAvailable} gates
 * M1 tests; M2 tests reuse the existing {@code @EnabledOnRedisSentinelAvailable}.</li>
 * <li>{@code MultiDbFailover} (this package) drives each driver's native operator-style failover API
 * (Jedis {@code MultiDbClient#setActiveDatabase}, Lettuce {@code StatefulRedisMultiDbConnection#switchTo}) to shift
 * routing between endpoints while both nodes stay up. Automatic failure detection and circuit-breaking are delegated to
 * the driver test suites; these tests focus on routing transparency through the Spring Data abstractions.</li>
 * <li>{@code resilience4j-circuitbreaker} is on the test classpath at version aligned with Jedis' optional declaration
 * (Jedis {@code MultiDbConfig} static init touches it).</li>
 * </ul>
 *
 * <h2>Scenario catalogue (numbering scheme {@code MD-<group>-<n>})</h2>
 *
 * Each scenario doubles as executable documentation: method names are sentence-form so reports read as docs, and the
 * test body (with comments stripped) should be a usable snippet for an end user.
 *
 * <h3>Group S1 &mdash; Wiring showcase (Mode M1, no faults)</h3>
 * <ul>
 * <li><b>MD-S1-1</b> &mdash; Configure {@link org.springframework.data.redis.connection.jedis.JedisConnectionFactory
 * JedisConnectionFactory} / {@link org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
 * LettuceConnectionFactory} from a {@link org.springframework.data.redis.connection.RedisMultiDbConfiguration}; assert
 * {@code ping()} returns "PONG" and {@code isMultiDbAware()} is {@code true}.</li>
 * <li><b>MD-S1-2</b> &mdash; {@code StringRedisTemplate} round-trip.</li>
 * <li><b>MD-S1-3</b> &mdash; {@code RedisTemplate} with default serializers (hash ops).</li>
 * <li><b>MD-S1-4</b> &mdash; {@code ReactiveRedisTemplate} round-trip (Lettuce only).</li>
 * </ul>
 *
 * <h3>Group S2 &mdash; Failover transparency (Mode M1)</h3>
 * <ul>
 * <li><b>MD-S2-1</b> &mdash; {@code StringRedisTemplate} ops remain transparent across a manual operator-style failover
 * (driver-native {@code setActiveDatabase} / {@code switchTo}); the template follows the shifted active endpoint with no
 * reconfiguration.</li>
 * <li><b>MD-S2-2</b> &mdash; Ops survive failback &mdash; a second manual failover returns routing to the original
 * endpoint and the template keeps working.</li>
 * <li><b>MD-S2-3</b> &mdash; {@code ReactiveRedisTemplate} ops remain transparent across manual failover (Lettuce
 * only).</li>
 * </ul>
 *
 * <h3>Group S3 &mdash; Pub/Sub integration (Mode M1)</h3>
 * <ul>
 * <li><b>MD-S3-1</b> &mdash; {@code RedisMessageListenerContainer} (Lettuce) subscribes through the multi-DB factory and
 * receives messages published over the same factory.</li>
 * <li><b>MD-S3-2</b> &mdash; {@code ReactiveRedisMessageListenerContainer} delivers over the multi-DB factory.</li>
 * <li><b>MD-S3-3</b> &mdash; Jedis {@code RedisMessageListenerContainer} delivers over the multi-DB factory. Failover
 * detection and subscription rebinding are driver concerns covered by the Jedis test suite.</li>
 * </ul>
 *
 * <h3>Group S4 &mdash; Mode M2: OSS read-replica showcase</h3>
 * <ul>
 * <li><b>MD-S4-1</b> &mdash; Two-factory wiring: read multi-DB over replicas + write standalone to master; SET via
 * write template, GET via read template after brief replication wait.</li>
 * <li><b>MD-S4-2</b> &mdash; Read template transparently follows a manual failover between replicas.</li>
 * <li><b>MD-S4-3</b> &mdash; Accidental write through the read factory surfaces {@code READONLY} as a
 * {@link org.springframework.data.redis.RedisSystemException RedisSystemException}; no infinite failover loop.</li>
 * </ul>
 *
 * <h3>Group S5 &mdash; Lifecycle and escape hatch (Mode M1, no faults)</h3>
 * <ul>
 * <li><b>MD-S5-1</b> &mdash; {@code destroy()} on a multi-DB-backed factory shuts down cleanly; second
 * {@code getConnection()} throws.</li>
 * <li><b>MD-S5-2</b> &mdash; {@code getRequiredNativeClient()} / native multi-DB client escape hatch reachable from
 * user code (proves users can drop down to the driver when needed).</li>
 * </ul>
 *
 * <h2>Out of scope (covered by the drivers, not here)</h2>
 *
 * Circuit-breaker threshold precision, retry/backoff timings, health-check strategy internals (e.g. {@code
 * LagAwareStrategy}), per-node {@code withWeight} / runtime weight reordering, per-node authentication propagation, pool
 * eviction (Jedis), shared-connection rebinding (Lettuce), customizer invocation count / argument shape (covered by
 * Phase D unit tests), concurrent-load resilience / throughput numbers, grace-period precision, {@code
 * failbackCheckInterval} timing, cluster + multi-DB.
 *
 * <h2>Test class layout</h2>
 *
 * <pre>
 * connection/multidb/
 * &#x251c;&#x2500; AbstractMultiDbConnectionIntegrationTests   (shared S1/S2/S5 scenarios)
 * &#x251c;&#x2500; jedis/JedisMultiDbIntegrationTests          (S1, S2, S5)
 * &#x251c;&#x2500; jedis/JedisMultiDbPubSubIntegrationTests    (S3-3)
 * &#x251c;&#x2500; jedis/JedisMultiDbReplicaIntegrationTests   (S4)
 * &#x251c;&#x2500; lettuce/LettuceMultiDbIntegrationTests      (S1, S2, S5)
 * &#x251c;&#x2500; lettuce/LettuceMultiDbPubSubIntegrationTests (S3-1, S3-2)
 * &#x2514;&#x2500; lettuce/LettuceMultiDbReplicaIntegrationTests (S4)
 * </pre>
 *
 * @author Tihomir Mateev
 */
package org.springframework.data.redis.connection.multidb;
