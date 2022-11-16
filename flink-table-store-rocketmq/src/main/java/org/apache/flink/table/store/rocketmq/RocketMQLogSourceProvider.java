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

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.store.CoreOptions.LogConsistency;
import org.apache.flink.table.store.CoreOptions.LogStartupMode;
import org.apache.flink.table.store.log.LogSourceProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;
import org.apache.rocketmq.flink.common.RocketMQOptions;
import org.apache.rocketmq.flink.source.RocketMQSource;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import static org.apache.flink.api.connector.source.Boundedness.CONTINUOUS_UNBOUNDED;
import static org.apache.flink.table.store.CoreOptions.LogStartupMode.FROM_TIMESTAMP;
import static org.apache.flink.table.store.CoreOptions.LogStartupMode.FULL;
import static org.apache.flink.table.store.CoreOptions.LogStartupMode.LATEST;
import static org.apache.rocketmq.flink.common.RocketMQOptions.CONSUMER_GROUP;
import static org.apache.rocketmq.flink.common.RocketMQOptions.NAME_SERVER_ADDRESS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_ACCESS_KEY;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_CONSUMER_POLL_MS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_END_TIME;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_SECRET_KEY;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_SQL;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_START_MESSAGE_OFFSET;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_START_TIME;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_START_TIME_MILLS;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_TAG;
import static org.apache.rocketmq.flink.common.RocketMQOptions.OPTIONAL_TIME_ZONE;
import static org.apache.rocketmq.flink.common.RocketMQOptions.TOPIC;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_OFFSET_EARLIEST;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_OFFSET_FROM_TIMESTAMP;
import static org.apache.rocketmq.flink.legacy.RocketMQConfig.CONSUMER_OFFSET_LATEST;

/** A rocketmq {@link LogSourceProvider}. */
public class RocketMQLogSourceProvider implements LogSourceProvider {

    private static final long serialVersionUID = 1L;
    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

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
    private final long pollTime;
    private final String consumerGroup;
    private final String nameServerAddress;
    private final String tag;
    private final String accessKey;
    private final String secretKey;
    private final String sql;
    private final long stopInMs;
    private final long startTime;
    private final long startMessageOffset;
    private final long partitionDiscoveryIntervalMs;
    private String consumerOffsetMode;
    private final long consumerOffsetTimestamp;

    public RocketMQLogSourceProvider(
            Properties properties,
            DataType physicalType,
            int[] primaryKey,
            @Nullable DeserializationSchema<RowData> primaryKeyDeserializer,
            DeserializationSchema<RowData> valueDeserializer,
            @Nullable int[][] projectFields,
            Configuration configuration,
            LogConsistency consistency,
            LogStartupMode scanMode,
            @Nullable Long timestampMills) {
        this.properties = properties;
        this.physicalType = physicalType;
        this.primaryKey = primaryKey;
        this.primaryKeyDeserializer = primaryKeyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.projectFields = projectFields;
        this.consistency = consistency;
        this.scanMode = scanMode;
        this.timestampMills = timestampMills;

        // Get config from context
       this.topic = configuration.getString(TOPIC);
       this.pollTime= configuration.getLong(OPTIONAL_CONSUMER_POLL_MS);
       this.consumerGroup = configuration.getString(CONSUMER_GROUP);
       this.nameServerAddress = configuration.getString(NAME_SERVER_ADDRESS);
       this.tag = configuration.getString(OPTIONAL_TAG);
       this.sql = configuration.getString(OPTIONAL_SQL);
        if (configuration.contains(OPTIONAL_SCAN_STARTUP_MODE)
                && (configuration.contains(OPTIONAL_START_MESSAGE_OFFSET)
                || configuration.contains(OPTIONAL_START_TIME_MILLS)
                || configuration.contains(OPTIONAL_START_TIME))) {
            throw new IllegalArgumentException(
                    String.format(
                            "cannot support these configs when %s has been set: [%s, %s, %s] !",
                            OPTIONAL_SCAN_STARTUP_MODE.key(),
                            OPTIONAL_START_MESSAGE_OFFSET.key(),
                            OPTIONAL_START_TIME.key(),
                            OPTIONAL_START_TIME_MILLS.key()));
        }
        this.startMessageOffset = configuration.getLong(OPTIONAL_START_MESSAGE_OFFSET);
        long startTimeMs = configuration.getLong(OPTIONAL_START_TIME_MILLS);
        String startDateTime = configuration.getString(OPTIONAL_START_TIME);
        String timeZone = configuration.getString(OPTIONAL_TIME_ZONE);
        this.accessKey = configuration.getString(OPTIONAL_ACCESS_KEY);
        this.secretKey = configuration.getString(OPTIONAL_SECRET_KEY);
        long startTime = startTimeMs;
        if (startTime == -1) {
            if (!StringUtils.isNullOrWhitespaceOnly(startDateTime)) {
                try {
                    startTime = parseDateString(startDateTime, timeZone);
                } catch (ParseException e) {
                    throw new RuntimeException(
                            String.format(
                                    "Incorrect datetime format: %s, pls use ISO-8601 "
                                            + "complete date plus hours, minutes and seconds format:%s.",
                                    startDateTime, DATE_FORMAT),
                            e);
                }
            }
        }
        long stopInMs = Long.MAX_VALUE;
        String endDateTime = configuration.getString(OPTIONAL_END_TIME);
        if (!StringUtils.isNullOrWhitespaceOnly(endDateTime)) {
            try {
                stopInMs = parseDateString(endDateTime, timeZone);
            } catch (ParseException e) {
                throw new RuntimeException(
                        String.format(
                                "Incorrect datetime format: %s, pls use ISO-8601 "
                                        + "complete date plus hours, minutes and seconds format:%s.",
                                endDateTime, DATE_FORMAT),
                        e);
            }
            Preconditions.checkArgument(
                    stopInMs >= startTime, "Start time should be less than stop time.");
        }
        this.startTime = startTime;
        this.stopInMs = stopInMs;
        this.partitionDiscoveryIntervalMs =
                configuration.getLong(OPTIONAL_PARTITION_DISCOVERY_INTERVAL_MS);
        this.consumerOffsetMode =
                configuration.getString(
                        RocketMQOptions.OPTIONAL_SCAN_STARTUP_MODE, CONSUMER_OFFSET_LATEST);
        this.consumerOffsetTimestamp =
                configuration.getLong(
                        RocketMQOptions.OPTIONAL_OFFSET_FROM_TIMESTAMP, System.currentTimeMillis());


    }

    @Override
    public RocketMQSource<RowData> createSource(@Nullable Map<Integer, Long> bucketOffsets) {
        adjustToRocketMQSourceConfig(bucketOffsets);
        return new RocketMQSource<>(
                pollTime,
                topic,
                consumerGroup,
                nameServerAddress,
                accessKey,
                secretKey,
                tag,
                sql,
                stopInMs,
                startTime,
                startMessageOffset < 0 ? 0 : startMessageOffset,
                partitionDiscoveryIntervalMs,
                CONTINUOUS_UNBOUNDED,
                new RocketMQLogDeserializationSchema(physicalType,
                        primaryKey,
                        primaryKeyDeserializer,
                        valueDeserializer,
                        projectFields),
                consumerOffsetMode,
                consumerOffsetTimestamp);
    }


    private void adjustToRocketMQSourceConfig(@Nullable Map<Integer, Long> bucketOffsets) {

        if (scanMode == FULL) {
            this.consumerOffsetMode = CONSUMER_OFFSET_EARLIEST;
        } else if (scanMode == LATEST) {
            this.consumerOffsetMode = CONSUMER_OFFSET_LATEST;
        } else if (scanMode == FROM_TIMESTAMP) {
            // TODO: 2022/11/16 support timestamp
            this.consumerOffsetMode = CONSUMER_OFFSET_FROM_TIMESTAMP;
        } else {
            throw new IllegalArgumentException("Unknown scan mode: " + scanMode);
        }
    }


    private Long parseDateString(String dateString, String timeZone) throws ParseException {
        FastDateFormat simpleDateFormat =
                FastDateFormat.getInstance(DATE_FORMAT, TimeZone.getTimeZone(timeZone));
        return simpleDateFormat.parse(dateString).getTime();
    }

}
