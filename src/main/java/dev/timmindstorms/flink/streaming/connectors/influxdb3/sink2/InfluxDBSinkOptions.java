/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Modified by timmindstorms on 11 December 2025.
 * Copyright (c) 2025 timmindstorms. All rights reserved.
 * Changes: 
 * - Migrated InfluxDB client to InfluxDB v3 (com.influxdb.v3.client).
 * - Removed legacy authentication (username/password/org).
 * - Added database configuration option.
 * - Updated package to dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.
 */
package dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2;

import com.influxdb.v3.client.InfluxDBClient;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

public final class InfluxDBSinkOptions {

        private InfluxDBSinkOptions() {
        }

        public static final ConfigOption<Boolean> WRITE_DATA_POINT_CHECKPOINT = ConfigOptions
                        .key("sink.influxDB.write.data_point.checkpoint")
                        .booleanType()
                        .defaultValue(false)
                        .withDescription(
                                        "Determines if the checkpoint data point should be written to InfluxDB or not.");

        public static final ConfigOption<Integer> WRITE_BUFFER_SIZE = ConfigOptions
                        .key("sink.influxDB.write.buffer.size")
                        .intType()
                        .defaultValue(1000)
                        .withDescription(
                                        "Size of the buffer to store the data before writing to InfluxDB.");

        public static final ConfigOption<String> INFLUXDB_URL = ConfigOptions.key("sink.influxDB.client.URL")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("InfluxDB Connection URL.");

        public static final ConfigOption<String> INFLUXDB_TOKEN = ConfigOptions.key("sink.influxDB.client.token")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("InfluxDB access token.");

        public static final ConfigOption<String> INFLUXDB_DATABASE = ConfigOptions.key("sink.influxDB.client.database")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("InfluxDB database name.");

        public static InfluxDBClient getInfluxDBClient(final Configuration configuration) {
                final String url = configuration.getString(INFLUXDB_URL);
                final String token = configuration.getString(INFLUXDB_TOKEN);
                final String database = configuration.getString(INFLUXDB_DATABASE);

                return InfluxDBClient.getInstance(url, token.toCharArray(), database);
        }
}
