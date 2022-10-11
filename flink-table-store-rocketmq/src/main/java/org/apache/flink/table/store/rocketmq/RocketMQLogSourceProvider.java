/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.rocketmq;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rocketmq.source.RocketMQSource;
import org.apache.flink.connector.rocketmq.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.rocketmq.source.reader.deserializer.RocketMQRecordDeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions.LogConsistency;
import org.apache.flink.table.store.CoreOptions.LogStartupMode;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.types.DataType;

import org.apache.rocketmq.common.TopicPartition;
import org.apache.rocketmq.flink.source.RocketMQSource;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.apache.rocketmq.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;

/** A rocketmq {@link LogSourceProvider}. */
public class RocketMQLogSourceProvider implements LogSourceProvider {

    private static final long serialVersionUID = 1L;

    private final String topic;

    private final Properties properties;

    private final DataType physicalType;

    private final int[] primaryKey;

    @Nullable private final DeserializationSchema<RowData> primaryKeyDeserializer;

    private final DeserializationSchema<RowData> valueDeserializer;

    @Nullable private final int[][] projectFields;

    private final LogConsistency consistency;

    private final LogStartupMode scanMode;

    @Nullable private final Long timestampMills;

    public RocketMQLogSourceProvider(
            String topic,
            Properties properties,
            DataType physicalType,
            int[] primaryKey,
            @Nullable DeserializationSchema<RowData> primaryKeyDeserializer,
            DeserializationSchema<RowData> valueDeserializer,
            @Nullable int[][] projectFields,
            LogConsistency consistency,
            LogStartupMode scanMode,
            @Nullable Long timestampMills) {
        this.topic = topic;
        this.properties = properties;
        this.physicalType = physicalType;
        this.primaryKey = primaryKey;
        this.primaryKeyDeserializer = primaryKeyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.projectFields = projectFields;
        this.consistency = consistency;
        this.scanMode = scanMode;
        this.timestampMills = timestampMills;
    }

    @Override
    public RocketMQSource<RowData> createSource(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (consistency) {
            case TRANSACTIONAL:
                // Add read committed for transactional consistency mode.
                properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_committed");
                break;
            case EVENTUAL:
                if (primaryKeyDeserializer == null) {
                    throw new IllegalArgumentException(
                            "Can not use EVENTUAL consistency mode for non-pk table.");
                }
                properties.setProperty(ISOLATION_LEVEL_CONFIG, "read_uncommitted");
                break;
        }

        return RocketMQSource.<RowData>builder()
                .setTopics(topic)
                .setStartingOffsets(toOffsetsInitializer(bucketOffsets))
                .setProperties(properties)
                .setDeserializer(createDeserializationSchema())
                .setGroupId(UUID.randomUUID().toString())
                .build();
    }

    @VisibleForTesting
    RocketMQRecordDeserializationSchema<RowData> createDeserializationSchema() {
        return RocketMQRecordDeserializationSchema.of(
                new RocketMQLogDeserializationSchema(
                        physicalType,
                        primaryKey,
                        primaryKeyDeserializer,
                        valueDeserializer,
                        projectFields));
    }

    private OffsetsInitializer toOffsetsInitializer(@Nullable Map<Integer, Long> bucketOffsets) {
        switch (scanMode) {
            case FULL:
                return bucketOffsets == null
                        ? OffsetsInitializer.earliest()
                        : OffsetsInitializer.offsets(toRocketMQOffsets(bucketOffsets));
            case LATEST:
                return OffsetsInitializer.latest();
            case FROM_TIMESTAMP:
                if (timestampMills == null) {
                    throw new NullPointerException(
                            "Must specify a timestamp if you choose timestamp startup mode.");
                }
                return OffsetsInitializer.timestamp(timestampMills);
            default:
                throw new UnsupportedOperationException("Unsupported mode: " + scanMode);
        }
    }

    private Map<TopicPartition, Long> toRocketMQOffsets(Map<Integer, Long> bucketOffsets) {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        bucketOffsets.forEach(
                (bucket, offset) -> offsets.put(new TopicPartition(topic, bucket), offset));
        return offsets;
    }
}
