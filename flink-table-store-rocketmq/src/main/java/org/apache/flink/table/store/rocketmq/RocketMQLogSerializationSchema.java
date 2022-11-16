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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema.InitializationContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions.LogChangelogMode;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.flink.types.RowKind;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.flink.sink2.writer.serializer.RocketMQSerializationSchema;

import javax.annotation.Nullable;

/**
 * A {@link RocketMQSerializationSchema} for the table in log store.
 */
public class RocketMQLogSerializationSchema implements RocketMQSerializationSchema<SinkRecord> {

    private static final long serialVersionUID = 1L;

    private final String topic;
    @Nullable
    private final SerializationSchema<RowData> primaryKeySerializer;
    private final SerializationSchema<RowData> valueSerializer;
    private final LogChangelogMode changelogMode;

    public RocketMQLogSerializationSchema(
            String topic,
            @Nullable SerializationSchema<RowData> primaryKeySerializer,
            SerializationSchema<RowData> valueSerializer,
            LogChangelogMode changelogMode) {
        this.topic = topic;
        this.primaryKeySerializer = primaryKeySerializer;
        this.valueSerializer = valueSerializer;
        this.changelogMode = changelogMode;
        if (changelogMode == LogChangelogMode.UPSERT && primaryKeySerializer == null) {
            throw new IllegalArgumentException(
                    "Can not use upsert changelog mode for non-pk table.");
        }
    }

    @Override
    public void open(InitializationContext context, RocketMQSinkContext sinkContext) throws Exception {

        if (primaryKeySerializer != null) {
            primaryKeySerializer.open(context);
        }
        valueSerializer.open(context);
        RocketMQSerializationSchema.super.open(context, sinkContext);
    }

    @Override
    public Message serialize(SinkRecord element, RocketMQSinkContext context, Long timestamp) {
        RowKind kind = element.row().getRowKind();

        byte[] primaryKeyBytes = null;
        byte[] valueBytes = null;
        if (primaryKeySerializer != null) {
            primaryKeyBytes = primaryKeySerializer.serialize(element.primaryKey());
            if (changelogMode == LogChangelogMode.ALL
                    || kind == RowKind.INSERT
                    || kind == RowKind.UPDATE_AFTER) {
                valueBytes = valueSerializer.serialize(element.row());
            }
        } else {
            valueBytes = valueSerializer.serialize(element.row());
        }
        return new Message(topic, null, null, valueBytes);
    }
}
