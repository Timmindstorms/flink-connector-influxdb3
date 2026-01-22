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
 * - Removed legacy authentication (username/password/org).
 * - Added setInfluxDBDatabase() configuration option.
 * - Updated sanityCheck() to reflect authentication changes.
 * - Updated package to dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.
 * Modified by timmindstorms on 22 January 2026.
 * Copyright (c) 2026 timmindstorms. All rights reserved.
 * Changes: 
 * - Added setSslRootsFilePath() configuration option.
 */
package dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2;

import org.apache.flink.configuration.Configuration;

import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.writer.InfluxDBSchemaSerializer;
import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.writer.InfluxDBWriter;

import static dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.InfluxDBSinkOptions.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The @builder class for
 * {@link org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSink} to
 * make it easier for the users to construct a {@link
 * org.apache.flink.streaming.connectors.influxdb.sink2.InfluxDBSink}.
 *
 * <p>
 * The following example shows the minimum setup to create a InfluxDBSink that
 * uses the Long
 * values from a former operator and sends it to an InfluxDB instance.
 *
 * <pre>{@code
 * InfluxDBSink<Long> influxDBSink = InfluxDBSink.builder()
 *         .setInfluxDBSchemaSerializer(new InfluxDBSerializer())
 *         .setInfluxDBUrl(getUrl())
 *         .setInfluxDBDatabase(getDatabase())
 *         .setInfluxDBToken(getToken())
 *         .build();
 * }</pre>
 *
 * <p>
 * To specify the batch size that has a significant influence on performance,
 * one can call {@link
 * #setWriteBufferSize(int)}.
 *
 * <p>
 * Check the Java docs of each individual methods to learn more about the
 * settings to build a
 * InfluxDBSource.
 */
public final class InfluxDBSinkBuilder<IN> {
    private InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private String influxDBUrl;
    private String influxDBToken;
    private String databaseName;
    private final Configuration configuration;

    InfluxDBSinkBuilder() {
        this.influxDBUrl = null;
        this.influxDBToken = null;
        this.databaseName = null;
        this.influxDBSchemaSerializer = null;
        this.configuration = new Configuration();
    }

    /**
     * Sets the InfluxDB url.
     *
     * @param influxDBUrl the url of the InfluxDB instance to send data to.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBUrl(final String influxDBUrl) {
        this.influxDBUrl = influxDBUrl;
        this.configuration.setString(INFLUXDB_URL, checkNotNull(influxDBUrl));
        return this;
    }

    /**
     * Sets the InfluxDB token.
     *
     * @param influxDBToken the token of the InfluxDB instance.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBToken(final String influxDBToken) {
        this.influxDBToken = influxDBToken;
        this.configuration.setString(INFLUXDB_TOKEN, checkNotNull(influxDBToken));
        return this;
    }

    /**
     * Sets the InfluxDB database name.
     *
     * @param databaseName the database name of the InfluxDB instance to store the
     *                     data
     *                     in.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setInfluxDBDatabase(final String databaseName) {
        this.databaseName = databaseName;
        this.configuration.setString(INFLUXDB_DATABASE, checkNotNull(databaseName));
        return this;
    }

    /**
     * Sets the ssl Roots File Path for the influx client.
     *
     * @param sslRootsFilePath the path to the ssl Roots File Path, the file should
     *                         be in pem format.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setSslRootsFilePath(final String sslRootsFilePath) {
        this.configuration.setString(SSL_ROOTS_FILE_PATH, checkNotNull(sslRootsFilePath));
        return this;
    }

    /**
     * Sets the {@link InfluxDBSchemaSerializer serializer} of the input type IN for
     * the
     * InfluxDBSink.
     *
     * @param influxDBSchemaSerializer the serializer for the input type.
     * @return this InfluxDBSourceBuilder.
     */
    public <T extends IN> InfluxDBSinkBuilder<T> setInfluxDBSchemaSerializer(
            final InfluxDBSchemaSerializer<T> influxDBSchemaSerializer) {
        checkNotNull(influxDBSchemaSerializer);
        final InfluxDBSinkBuilder<T> sinkBuilder = (InfluxDBSinkBuilder<T>) this;
        sinkBuilder.influxDBSchemaSerializer = influxDBSchemaSerializer;
        return sinkBuilder;
    }

    /**
     * Sets if the InfluxDBSink should write checkpoint data points to InfluxDB.
     *
     * @param shouldWrite boolean if checkpoint should be written.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> addCheckpointDataPoint(final boolean shouldWrite) {
        this.configuration.setBoolean(WRITE_DATA_POINT_CHECKPOINT, shouldWrite);
        return this;
    }

    /**
     * Sets the buffer size of the {@link InfluxDBWriter}. This also determines the
     * number of {@link
     * com.influxdb.v3.client.Point} send to the InfluxDB instance per
     * request.
     *
     * @param bufferSize size of the buffer.
     * @return this InfluxDBSinkBuilder.
     */
    public InfluxDBSinkBuilder<IN> setWriteBufferSize(final int bufferSize) {
        if (bufferSize <= 0) {
            throw new IllegalArgumentException("The buffer size should be greater than 0.");
        }
        this.configuration.setInteger(WRITE_BUFFER_SIZE, bufferSize);
        return this;
    }

    /**
     * Build the
     * {@link dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.InfluxDBSink}.
     *
     * @return a InfluxDBSink with the settings made for this builder.
     */
    public InfluxDBSink<IN> build() {
        this.sanityCheck();
        return new InfluxDBSink<>(this.influxDBSchemaSerializer, this.configuration);
    }

    // ------------- private helpers --------------

    /**
     * Checks if the SchemaSerializer and the influxDBConfig are not null and set.
     */
    private void sanityCheck() {
        // Check required settings.
        checkNotNull(this.influxDBUrl, "The InfluxDB URL is required but not provided.");
        checkNotNull(
                this.influxDBToken,
                "The InfluxDB token is required");
        checkNotNull(this.databaseName, "The Database name is required but not provided.");
        checkNotNull(
                this.influxDBSchemaSerializer,
                "Serialization schema is required but not provided.");
    }
}
