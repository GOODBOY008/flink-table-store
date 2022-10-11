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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.rocketmq.FlinkRocketMQException;
import org.apache.flink.streaming.connectors.rocketmq.FlinkRocketMQProducer;
import org.apache.flink.streaming.connectors.rocketmq.RocketMQSerializationSchema;
import org.apache.flink.table.store.table.sink.LogSinkFunction;
import org.apache.flink.table.store.table.sink.SinkRecord;

import org.apache.rocketmq.clients.producer.Callback;
import org.apache.rocketmq.clients.producer.ProducerRecord;

import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * A {@link FlinkRocketMQProducer} which implements {@link LogSinkFunction} to register {@link
 * WriteCallback}.
 */
public class RocketMQSinkFunction extends RocketMQRowDataSink<SinkRecord> implements LogSinkFunction {

    private WriteCallback writeCallback;

    /**
     * Creates a {@link RocketMQSinkFunction} for a given topic. The sink produces its input to the
     * topic. It accepts a {@link RocketMQSerializationSchema} for serializing records to a {@link
     * ProducerRecord}, including partitioning information.
     *
     * @param defaultTopic The default topic to write data to
     * @param serializationSchema A serializable serialization schema for turning user objects into
     *     a rocketmq-consumable byte[] supporting key/value messages
     * @param producerConfig Configuration properties for the RocketMQProducer. 'bootstrap.servers.' is
     *     the only required argument.
     * @param semantic Defines semantic that will be used by this producer (see {@link
     *     RocketMQSinkFunction.Semantic}).
     */
    public RocketMQSinkFunction(
            String defaultTopic,
            RocketMQSerializationSchema<SinkRecord> serializationSchema,
            Properties producerConfig,
            RocketMQSinkFunction.Semantic semantic) {
        super(defaultTopic, serializationSchema, producerConfig, semantic);
    }

    public void setWriteCallback(WriteCallback writeCallback) {
        this.writeCallback = writeCallback;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        Callback baseCallback = requireNonNull(callback);
        callback =
                (metadata, exception) -> {
                    if (writeCallback != null) {
                        writeCallback.onCompletion(metadata.partition(), metadata.offset());
                    }
                    baseCallback.onCompletion(metadata, exception);
                };
    }

    @Override
    public void flush() throws FlinkRocketMQException {
        super.preCommit(super.currentTransaction());
    }
}
