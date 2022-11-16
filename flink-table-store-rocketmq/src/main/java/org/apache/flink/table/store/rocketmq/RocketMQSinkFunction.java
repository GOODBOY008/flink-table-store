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

import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.table.store.table.sink.SinkRecord;
import org.apache.rocketmq.flink.sink2.RocketMQSink;
import org.apache.rocketmq.flink.sink2.writer.serializer.RocketMQSerializationSchema;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * A {@link RocketMQSink} which implements {@link LogSinkFunction} to register {@link
 * WriteCallback}.
 */
public class RocketMQSinkFunction extends RocketMQSink<SinkRecord> implements LogSinkFunction {

    private WriteCallback writeCallback;

    public RocketMQSinkFunction(
            DeliveryGuarantee deliveryGuarantee,
            Properties rabbitmqProducerConfig,
            String producerGroup,
            RocketMQSerializationSchema<SinkRecord> recordSerializer) {
        super(
                deliveryGuarantee,
                rabbitmqProducerConfig,
                producerGroup,
                recordSerializer);
    }

    @Override
    public void setWriteCallback(WriteCallback writeCallback) {

    }

    @Override
    public void flush() throws Exception {
    }
}
