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
 * - Updated imports to reflex new package structure.
 * - Updated package to dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.
 */
package dev.timmindstorms.flink.streaming.connectors.influxdb3.sink;

import com.influxdb.v3.client.Point;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.commiter.InfluxDBCommittableSerializer;
import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.commiter.InfluxDBCommitter;
import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.writer.InfluxDBPointSerializer;
import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.writer.InfluxDBSchemaSerializer;
import dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.writer.InfluxDBWriter;

import java.util.List;
import java.util.Optional;

/**
 * This Sink implementation of InfluxDB/Line Protocol. Please use a
 * {@link InfluxDBSinkBuilder} to
 * construct a {@link InfluxDBSink}. The following example shows how to create
 * an InfluxDBSink
 * having records of <code>Long</code> as input type.
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
 * See {@link InfluxDBSinkBuilder} for more details.
 *
 * @param <IN> type of the input of the sink.
 */
public final class InfluxDBSink<IN> implements Sink<IN, Long, Point, Void> {

    private final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer;
    private final Configuration configuration;

    InfluxDBSink(
            final InfluxDBSchemaSerializer<IN> influxDBSchemaSerializer,
            final Configuration configuration) {
        this.influxDBSchemaSerializer = influxDBSchemaSerializer;
        this.configuration = configuration;
    }

    /**
     * Get a influxDBSinkBuilder to build a {@link InfluxDBSink}.
     *
     * @return a InfluxDB sink builder.
     */
    public static <IN> InfluxDBSinkBuilder<IN> builder() {
        return new InfluxDBSinkBuilder<>();
    }

    @Override
    public SinkWriter<IN, Long, Point> createWriter(
            final InitContext initContext, final List<Point> list) {
        final InfluxDBWriter<IN> writer = new InfluxDBWriter<>(this.influxDBSchemaSerializer, this.configuration);
        writer.setProcessingTimerService(initContext.getProcessingTimeService());
        return writer;
    }

    @Override
    public Optional<Committer<Long>> createCommitter() {
        return Optional.of(new InfluxDBCommitter(this.configuration));
    }

    @Override
    public Optional<SimpleVersionedSerializer<Long>> getCommittableSerializer() {
        return Optional.of(new InfluxDBCommittableSerializer());
    }

    @Override
    public Optional<SimpleVersionedSerializer<Point>> getWriterStateSerializer() {
        return Optional.of(new InfluxDBPointSerializer());
    }

    @Override
    public Optional<GlobalCommitter<Long, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }
}
