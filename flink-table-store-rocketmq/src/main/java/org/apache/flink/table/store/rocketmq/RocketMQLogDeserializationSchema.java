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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rocketmq.RocketMQDeserializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.store.utils.ProjectedRowData;
import org.apache.flink.table.store.utils.Projection;
import org.apache.flink.table.store.utils.RowDataUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.rocketmq.clients.consumer.ConsumerRecord;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.flink.source.reader.deserializer.RocketMQDeserializationSchema;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/** A {@link RocketMQDeserializationSchema} for the table with primary key in log store. */
public class RocketMQLogDeserializationSchema implements RocketMQDeserializationSchema<RowData> {

    @Override
    public void open(InitializationContext context) {
        RocketMQDeserializationSchema.super.open(context);
    }

    @Override
    public void deserialize(List<MessageExt> record, Collector<RowData> out) throws IOException {

    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return null;
    }
}
