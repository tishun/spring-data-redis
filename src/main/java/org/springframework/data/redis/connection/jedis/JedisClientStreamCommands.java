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

import redis.clients.jedis.BuilderFactory;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XClaimParams;
import redis.clients.jedis.params.XPendingParams;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.params.XTrimParams;
import redis.clients.jedis.resps.StreamConsumerInfo;
import redis.clients.jedis.resps.StreamGroupInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.RedisStreamCommands;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.util.Assert;

/**
 * @author Tihomir Mateev
 * @since 4.1
 */
@NullUnmarked
class JedisClientStreamCommands implements RedisStreamCommands {

	private final JedisClientConnection connection;

	JedisClientStreamCommands(@NonNull JedisClientConnection connection) {
		this.connection = connection;
	}

	@Override
	public Long xAck(byte @NonNull [] key, @NonNull String group, @NonNull RecordId @NonNull... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(group, "Group name must not be null or empty");
		Assert.notNull(recordIds, "recordIds must not be null");

		return connection.execute(
				client -> client.xack(key, JedisConverters.toBytes(group), StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))),
				pipeline -> pipeline.xack(key, JedisConverters.toBytes(group), StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))));
	}

	@Override
	public RecordId xAdd(@NonNull MapRecord<byte[], byte[], byte[]> record, @NonNull XAddOptions options) {

		Assert.notNull(record, "Record must not be null");
		Assert.notNull(record.getStream(), "Stream must not be null");

		XAddParams params = StreamConverters.toXAddParams(record.getId(), options);

		byte[] result = connection.execute(
				client -> client.xadd(record.getStream(), record.getValue(), params),
				pipeline -> pipeline.xadd(record.getStream(), record.getValue(), params));

		return result != null ? RecordId.of(JedisConverters.toString(result)) : null;
	}

	@Override
	public List<@NonNull RecordId> xClaimJustId(byte @NonNull [] key, @NonNull String group, @NonNull String newOwner,
			@NonNull XClaimOptions options) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(group, "Group must not be null");
		Assert.notNull(newOwner, "NewOwner must not be null");

		XClaimParams params = StreamConverters.toXClaimParams(options);

		List<byte[]> result = connection.execute(
				client -> client.xclaimJustId(key, JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner),
						options.getMinIdleTime().toMillis(), params, StreamConverters.entryIdsToBytes(options.getIds())),
				pipeline -> pipeline.xclaimJustId(key, JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner),
						options.getMinIdleTime().toMillis(), params, StreamConverters.entryIdsToBytes(options.getIds())));

		if (result == null) {
			return null;
		}

		List<RecordId> converted = new ArrayList<>(result.size());
		for (byte[] item : result) {
			converted.add(RecordId.of(JedisConverters.toString(item)));
		}
		return converted;
	}

	@Override
	public List<@NonNull ByteRecord> xClaim(byte @NonNull [] key, @NonNull String group, @NonNull String newOwner,
			@NonNull XClaimOptions options) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(group, "Group must not be null");
		Assert.notNull(newOwner, "NewOwner must not be null");

		XClaimParams params = StreamConverters.toXClaimParams(options);

		Object result = connection.execute(
				client -> client.xclaim(key, JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner),
						options.getMinIdleTime().toMillis(), params, StreamConverters.entryIdsToBytes(options.getIds())),
				pipeline -> pipeline.xclaim(key, JedisConverters.toBytes(group), JedisConverters.toBytes(newOwner),
						options.getMinIdleTime().toMillis(), params, StreamConverters.entryIdsToBytes(options.getIds())));

		return result != null ? StreamConverters.convertToByteRecord(key, result) : null;
	}

	@Override
	public Long xDel(byte @NonNull [] key, @NonNull RecordId @NonNull... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(recordIds, "recordIds must not be null");

		return connection.execute(
				client -> client.xdel(key, StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))),
				pipeline -> pipeline.xdel(key, StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))));
	}

	@Override
	public List<StreamEntryDeletionResult> xDelEx(byte @NonNull [] key, @NonNull XDelOptions options,
			@NonNull RecordId @NonNull... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(options, "Options must not be null");
		Assert.notNull(recordIds, "recordIds must not be null");

		List<redis.clients.jedis.resps.StreamEntryDeletionResult> result = connection.execute(
				client -> client.xdelex(key, StreamConverters.toStreamDeletionPolicy(options),
						StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))),
				pipeline -> pipeline.xdelex(key, StreamConverters.toStreamDeletionPolicy(options),
						StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))));

		return result != null ? StreamConverters.toStreamEntryDeletionResults(result) : null;
	}

	@Override
	public List<StreamEntryDeletionResult> xAckDel(byte @NonNull [] key, @NonNull String group, @NonNull XDelOptions options,
			@NonNull RecordId @NonNull... recordIds) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(group, "Group must not be null");
		Assert.notNull(options, "Options must not be null");
		Assert.notNull(recordIds, "recordIds must not be null");

		List<redis.clients.jedis.resps.StreamEntryDeletionResult> result = connection.execute(
				client -> client.xackdel(key, JedisConverters.toBytes(group), StreamConverters.toStreamDeletionPolicy(options),
						StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))),
				pipeline -> pipeline.xackdel(key, JedisConverters.toBytes(group), StreamConverters.toStreamDeletionPolicy(options),
						StreamConverters.entryIdsToBytes(Arrays.asList(recordIds))));

		return result != null ? StreamConverters.toStreamEntryDeletionResults(result) : null;
	}

	@Override
	public String xGroupCreate(byte @NonNull [] key, @NonNull String groupName, @NonNull ReadOffset readOffset) {
		return xGroupCreate(key, groupName, readOffset, false);
	}

	@Override
	public String xGroupCreate(byte @NonNull [] key, @NonNull String groupName, @NonNull ReadOffset readOffset,
			boolean mkStream) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(groupName, "Group name must not be null or empty");
		Assert.notNull(readOffset, "ReadOffset must not be null");

		return connection.execute(
				client -> client.xgroupCreate(key, JedisConverters.toBytes(groupName), JedisConverters.toBytes(readOffset.getOffset()), mkStream),
				pipeline -> pipeline.xgroupCreate(key, JedisConverters.toBytes(groupName), JedisConverters.toBytes(readOffset.getOffset()), mkStream));
	}

	@Override
	public Boolean xGroupDelConsumer(byte @NonNull [] key, @NonNull Consumer consumer) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(consumer, "Consumer must not be null");

		Long result = connection.execute(
				client -> client.xgroupDelConsumer(key, JedisConverters.toBytes(consumer.getGroup()), JedisConverters.toBytes(consumer.getName())),
				pipeline -> pipeline.xgroupDelConsumer(key, JedisConverters.toBytes(consumer.getGroup()), JedisConverters.toBytes(consumer.getName())));

		return result != null ? result > 0 : null;
	}

	@Override
	public Boolean xGroupDestroy(byte @NonNull [] key, @NonNull String groupName) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(groupName, "Group name must not be null or empty");

		Long result = connection.execute(
				client -> client.xgroupDestroy(key, JedisConverters.toBytes(groupName)),
				pipeline -> pipeline.xgroupDestroy(key, JedisConverters.toBytes(groupName)));

		return result != null ? result > 0 : null;
	}

	@Override
	public StreamInfo.XInfoStream xInfo(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		Object result = connection.execute(
				client -> client.xinfoStream(key),
				pipeline -> pipeline.xinfoStream(key));

		if (result == null) {
			return null;
		}

		redis.clients.jedis.resps.StreamInfo streamInfo = BuilderFactory.STREAM_INFO.build(result);
		return StreamInfo.XInfoStream.fromList(StreamConverters.mapToList(streamInfo.getStreamInfo()));
	}

	@Override
	public StreamInfo.XInfoGroups xInfoGroups(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		Object result = connection.execute(
				client -> client.xinfoGroups(key),
				pipeline -> pipeline.xinfoGroups(key));

		if (result == null) {
			return null;
		}

		List<StreamGroupInfo> streamGroupInfos = BuilderFactory.STREAM_GROUP_INFO_LIST.build(result);
		List<Object> sources = new ArrayList<>();
		streamGroupInfos
				.forEach(streamGroupInfo -> sources.add(StreamConverters.mapToList(streamGroupInfo.getGroupInfo())));
		return StreamInfo.XInfoGroups.fromList(sources);
	}

	@Override
	public StreamInfo.XInfoConsumers xInfoConsumers(byte @NonNull [] key, @NonNull String groupName) {

		Assert.notNull(key, "Key must not be null");
		Assert.hasText(groupName, "Group name must not be null or empty");

		Object result = connection.execute(
				client -> client.xinfoConsumers(key, JedisConverters.toBytes(groupName)),
				pipeline -> pipeline.xinfoConsumers(key, JedisConverters.toBytes(groupName)));

		if (result == null) {
			return null;
		}

		List<StreamConsumerInfo> streamConsumersInfos = BuilderFactory.STREAM_CONSUMER_INFO_LIST.build(result);
		List<Object> sources = new ArrayList<>();
		streamConsumersInfos.forEach(
				streamConsumersInfo -> sources.add(StreamConverters.mapToList(streamConsumersInfo.getConsumerInfo())));
		return StreamInfo.XInfoConsumers.fromList(groupName, sources);
	}

	@Override
	public Long xLen(byte @NonNull [] key) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(
				client -> client.xlen(key),
				pipeline -> pipeline.xlen(key));
	}

	@Override
	public PendingMessagesSummary xPending(byte @NonNull [] key, @NonNull String groupName) {

		Assert.notNull(key, "Key must not be null");

		Object result = connection.execute(
				client -> client.xpending(key, JedisConverters.toBytes(groupName)),
				pipeline -> pipeline.xpending(key, JedisConverters.toBytes(groupName)));

		return result != null ? StreamConverters.toPendingMessagesSummary(groupName, result) : null;
	}

	@Override
	public PendingMessages xPending(byte @NonNull [] key, @NonNull String groupName, @NonNull XPendingOptions options) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(groupName, "GroupName must not be null");

		Range<@NonNull String> range = (Range<String>) options.getRange();
		XPendingParams xPendingParams = StreamConverters.toXPendingParams(options);

		Object result = connection.execute(
				client -> client.xpending(key, JedisConverters.toBytes(groupName), xPendingParams),
				pipeline -> pipeline.xpending(key, JedisConverters.toBytes(groupName), xPendingParams));

		return result != null ? StreamConverters.toPendingMessages(groupName, range,
				BuilderFactory.STREAM_PENDING_ENTRY_LIST.build(result)) : null;
	}

	@Override
	public List<@NonNull ByteRecord> xRange(byte @NonNull [] key, @NonNull Range<@NonNull String> range, @NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		int count = limit.isUnlimited() ? Integer.MAX_VALUE : limit.getCount();

		Object result = connection.execute(
				client -> client.xrange(key, JedisConverters.toBytes(StreamConverters.getLowerValue(range)),
						JedisConverters.toBytes(StreamConverters.getUpperValue(range)), count),
				pipeline -> pipeline.xrange(key, JedisConverters.toBytes(StreamConverters.getLowerValue(range)),
						JedisConverters.toBytes(StreamConverters.getUpperValue(range)), count));

		return result != null ? StreamConverters.convertToByteRecord(key, result) : null;
	}

	@Override
	public List<@NonNull ByteRecord> xRead(@NonNull StreamReadOptions readOptions,
			@NonNull StreamOffset<byte[]> @NonNull... streams) {

		Assert.notNull(readOptions, "StreamReadOptions must not be null");
		Assert.notNull(streams, "StreamOffsets must not be null");

		XReadParams params = StreamConverters.toXReadParams(readOptions);

		List<?> result = connection.execute(
				client -> client.xread(params, StreamConverters.toStreamOffsets(streams)),
				pipeline -> pipeline.xread(params, StreamConverters.toStreamOffsets(streams)));

		return result != null ? StreamConverters.convertToByteRecords(result) : Collections.emptyList();
	}

	@SafeVarargs
    @Override
	public final List<@NonNull ByteRecord> xReadGroup(@NonNull Consumer consumer, @NonNull StreamReadOptions readOptions,
                                                      @NonNull StreamOffset<byte[]> @NonNull ... streams) {

		Assert.notNull(consumer, "Consumer must not be null");
		Assert.notNull(readOptions, "StreamReadOptions must not be null");
		Assert.notNull(streams, "StreamOffsets must not be null");

		XReadGroupParams params = StreamConverters.toXReadGroupParams(readOptions);

		List<?> result = connection.execute(
				client -> client.xreadGroup(JedisConverters.toBytes(consumer.getGroup()),
						JedisConverters.toBytes(consumer.getName()), params, StreamConverters.toStreamOffsets(streams)),
				pipeline -> pipeline.xreadGroup(JedisConverters.toBytes(consumer.getGroup()),
						JedisConverters.toBytes(consumer.getName()), params, StreamConverters.toStreamOffsets(streams)));

		return result != null ? StreamConverters.convertToByteRecords(result) : Collections.emptyList();
	}

	@Override
	public List<@NonNull ByteRecord> xRevRange(byte @NonNull [] key, @NonNull Range<@NonNull String> range, @NonNull Limit limit) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(range, "Range must not be null");
		Assert.notNull(limit, "Limit must not be null");

		int count = limit.isUnlimited() ? Integer.MAX_VALUE : limit.getCount();

		Object result = connection.execute(
				client -> client.xrevrange(key, JedisConverters.toBytes(StreamConverters.getUpperValue(range)),
						JedisConverters.toBytes(StreamConverters.getLowerValue(range)), count),
				pipeline -> pipeline.xrevrange(key, JedisConverters.toBytes(StreamConverters.getUpperValue(range)),
						JedisConverters.toBytes(StreamConverters.getLowerValue(range)), count));

		return result != null ? StreamConverters.convertToByteRecord(key, result) : null;
	}

	@Override
	public Long xTrim(byte @NonNull [] key, long count) {
		return xTrim(key, count, false);
	}

	@Override
	public Long xTrim(byte @NonNull [] key, long count, boolean approximateTrimming) {

		Assert.notNull(key, "Key must not be null");

		return connection.execute(
				client -> client.xtrim(key, count, approximateTrimming),
				pipeline -> pipeline.xtrim(key, count, approximateTrimming));
	}

	@Override
	public Long xTrim(byte @NonNull [] key, @NonNull XTrimOptions options) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(options, "XTrimOptions must not be null");

		XTrimParams xTrimParams = StreamConverters.toXTrimParams(options);

		return connection.execute(
				client -> client.xtrim(key, xTrimParams),
				pipeline -> pipeline.xtrim(key, xTrimParams));
	}

}
