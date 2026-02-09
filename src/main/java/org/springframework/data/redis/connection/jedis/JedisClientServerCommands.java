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

import redis.clients.jedis.args.SaveMode;
import redis.clients.jedis.params.MigrateParams;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisServerCommands;
import org.springframework.data.redis.connection.convert.Converters;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.util.Assert;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientServerCommands implements RedisServerCommands {

	private final JedisClientConnection connection;

	JedisClientServerCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public void bgReWriteAof() {
		connection.execute("BGREWRITEAOF");
	}

	@Override
	public void bgSave() {
		connection.execute("BGSAVE");
	}

	@Override
	public Long lastSave() {
		return (Long) connection.execute("LASTSAVE");
	}

	@Override
	public void save() {
		connection.execute("SAVE");
	}

	@Override
	public Long dbSize() {
		return (Long) connection.execute("DBSIZE");
	}

	@Override
	public void flushDb() {
		connection.execute("FLUSHDB");
	}

	@Override
	public void flushDb(@NonNull FlushOption option) {
		connection.execute("FLUSHDB", JedisConverters.toBytes(JedisConverters.toFlushMode(option).toString()));
	}

	@Override
	public void flushAll() {
		connection.execute("FLUSHALL");
	}

	@Override
	public void flushAll(@NonNull FlushOption option) {
		connection.execute("FLUSHALL", JedisConverters.toBytes(JedisConverters.toFlushMode(option).toString()));
	}

	@Override
	public Properties info() {
		String result = JedisConverters.toString((byte[]) connection.execute("INFO"));
		return result != null ? JedisConverters.toProperties(result) : new Properties();
	}

	@Override
	public Properties info(@NonNull String section) {

		Assert.notNull(section, "Section must not be null");

		String result = JedisConverters.toString((byte[]) connection.execute("INFO", JedisConverters.toBytes(section)));
		return result != null ? JedisConverters.toProperties(result) : new Properties();
	}

	@Override
	public void shutdown() {
		connection.execute("SHUTDOWN");
	}

	@Override
	public void shutdown(@Nullable ShutdownOption option) {

		if (option == null) {
			shutdown();
			return;
		}

		SaveMode saveMode = (option == ShutdownOption.NOSAVE) ? SaveMode.NOSAVE : SaveMode.SAVE;
		connection.execute("SHUTDOWN", JedisConverters.toBytes(saveMode.toString()));
	}

	@Override
	public Properties getConfig(@NonNull String pattern) {

		Assert.notNull(pattern, "Pattern must not be null");

		@SuppressWarnings("unchecked")
		List<byte[]> result = (List<byte[]>) connection.execute(
				client -> client.sendCommand(redis.clients.jedis.Protocol.Command.CONFIG, JedisConverters.toBytes("GET"), JedisConverters.toBytes(pattern)),
				pipeline -> pipeline.sendCommand(redis.clients.jedis.Protocol.Command.CONFIG, JedisConverters.toBytes("GET"), JedisConverters.toBytes(pattern)));

		if (result == null) {
			return new Properties();
		}

		List<String> stringResult = result.stream()
				.map(JedisConverters::toString)
				.toList();

		return Converters.toProperties(stringResult);
	}

	@Override
	public void setConfig(@NonNull String param, @NonNull String value) {

		Assert.notNull(param, "Parameter must not be null");
		Assert.notNull(value, "Value must not be null");

		connection.execute("CONFIG", JedisConverters.toBytes("SET"), JedisConverters.toBytes(param), JedisConverters.toBytes(value));
	}

	@Override
	public void resetConfigStats() {
		connection.execute("CONFIG", JedisConverters.toBytes("RESETSTAT"));
	}

	@Override
	public void rewriteConfig() {
		connection.execute("CONFIG", JedisConverters.toBytes("REWRITE"));
	}

	@Override
	public Long time(@NonNull TimeUnit timeUnit) {

		Assert.notNull(timeUnit, "TimeUnit must not be null");

		@SuppressWarnings("unchecked")
		List<byte[]> result = (List<byte[]>) connection.execute(
				client -> client.sendCommand(redis.clients.jedis.Protocol.Command.TIME, new byte[0][]),
				pipeline -> pipeline.sendCommand(redis.clients.jedis.Protocol.Command.TIME, new byte[0][]));

		if (result == null) {
			return null;
		}

		List<String> stringResult = result.stream()
				.map(JedisConverters::toString)
				.toList();

		return JedisConverters.toTime(stringResult, timeUnit);
	}

	@Override
	public void killClient(@NonNull String host, int port) {

		Assert.hasText(host, "Host for 'CLIENT KILL' must not be 'null' or 'empty'");

		connection.execute("CLIENT", JedisConverters.toBytes("KILL"), JedisConverters.toBytes("%s:%s".formatted(host, port)));
	}

	@Override
	public void setClientName(byte @NonNull [] name) {

		Assert.notNull(name, "Name must not be null");

		connection.execute("CLIENT", JedisConverters.toBytes("SETNAME"), name);
	}

	@Override
	public String getClientName() {
		return JedisConverters.toString((byte[]) connection.execute("CLIENT", JedisConverters.toBytes("GETNAME")));
	}

	@Override
	public List<@NonNull RedisClientInfo> getClientList() {
		String result = JedisConverters.toString((byte[]) connection.execute("CLIENT", JedisConverters.toBytes("LIST")));
		return result != null ? JedisConverters.toListOfRedisClientInformation(result) : null;
	}

	@Override
	public void replicaOf(@NonNull String host, int port) {

		Assert.hasText(host, "Host must not be null for 'REPLICAOF' command");

		connection.execute("REPLICAOF", JedisConverters.toBytes(host), JedisConverters.toBytes(String.valueOf(port)));
	}

	@Override
	public void replicaOfNoOne() {
		connection.execute("REPLICAOF", JedisConverters.toBytes("NO"), JedisConverters.toBytes("ONE"));
	}

	@Override
	public void migrate(byte @NonNull [] key, @NonNull RedisNode target, int dbIndex, @Nullable MigrateOption option) {
		migrate(key, target, dbIndex, option, Long.MAX_VALUE);
	}

	@Override
	public void migrate(byte @NonNull [] key, @NonNull RedisNode target, int dbIndex, @Nullable MigrateOption option,
			long timeout) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(target, "Target node must not be null");

		int timeoutToUse = timeout <= Integer.MAX_VALUE ? (int) timeout : Integer.MAX_VALUE;

		MigrateParams params = new MigrateParams();
		if (option != null) {
			if (option == MigrateOption.COPY) {
				params.copy();
			} else if (option == MigrateOption.REPLACE) {
				params.replace();
			}
		}

		connection.execute(
				client -> client.migrate(target.getRequiredHost(), target.getRequiredPort(), timeoutToUse, params, key),
				pipeline -> pipeline.migrate(target.getRequiredHost(), target.getRequiredPort(), timeoutToUse, params, key));
	}

}
