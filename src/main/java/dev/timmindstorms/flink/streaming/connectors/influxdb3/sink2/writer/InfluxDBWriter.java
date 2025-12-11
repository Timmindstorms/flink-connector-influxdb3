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
 * - writeElementsOf now writes using the client directly without the WriteApi.
 * - Updated imports to reflex new package structure.
 * - Updated package to dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.writer.
 */
package dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.writer;

import com.influxdb.v3.client.InfluxDBClient;

import com.influxdb.v3.client.write.WritePrecision;
import com.influxdb.v3.client.Point;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static dev.timmindstorms.flink.streaming.connectors.influxdb3.sink2.InfluxDBSinkOptions.*;

public class InfluxDBWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBWriter.class);

    private final int bufferSize;
    private final boolean writeCheckpoint;
    private long lastTimestamp = 0;
    private final List<Point> elements;
    private ProcessingTimeService processingTimerService;
    private final InfluxDBSchemaSerializer<IN> schemaSerializer;
    private final InfluxDBClient influxDBClient;

    public InfluxDBWriter(
            final InfluxDBSchemaSerializer<IN> schemaSerializer,
            final Configuration configuration) {
        this.schemaSerializer = schemaSerializer;
        this.bufferSize = configuration.getInteger(WRITE_BUFFER_SIZE);
        this.elements = new ArrayList<>(this.bufferSize);
        this.writeCheckpoint = configuration.getBoolean(WRITE_DATA_POINT_CHECKPOINT);
        this.influxDBClient = getInfluxDBClient(configuration);
    }

    public void setProcessingTimerService(final ProcessingTimeService processingTimerService) {
        this.processingTimerService = processingTimerService;
    }

    /**
     * This method calls the InfluxDB write API whenever the element list reaches
     * the {@link
     * #bufferSize}. It keeps track of the latest timestamp of each element. It
     * compares the latest
     * timestamp with the context.timestamp() and takes the bigger (latest)
     * timestamp.
     *
     * @param in      incoming data
     * @param context current Flink context
     * @see org.apache.flink.api.connector.sink2.SinkWriter.Context
     */
    @Override
    public void write(IN in, Context context) throws IOException, InterruptedException {
        LOG.trace("Adding elements to buffer. Buffer size: {}", this.elements.size());
        this.elements.add(this.schemaSerializer.serialize(in, context));

        if (this.elements.size() == this.bufferSize) {
            LOG.debug("Buffer size reached preparing to write the elements.");
            this.writeCurrentElements();
        }
        if (context.timestamp() != null) {
            this.lastTimestamp = Math.max(this.lastTimestamp, context.timestamp());
        }

    }

    @Override
    public void flush(boolean flush) throws IOException, InterruptedException {
        if (this.lastTimestamp == 0)
            return;

        commit(Collections.singletonList(this.lastTimestamp));
    }

    public void commit(final List<Long> committables) {
        if (this.writeCheckpoint) {
            LOG.debug("A checkpoint is set.");
            Optional<Long> lastTimestamp = Optional.empty();
            if (committables.size() >= 1) {
                lastTimestamp = Optional.ofNullable(committables.get(committables.size() - 1));
            }
            lastTimestamp.ifPresent(this::writeCheckpointDataPoint);
        }
    }

    private void writeCheckpointDataPoint(final Long timestamp) {
        final Point point = new Point("checkpoint")
                .setField("checkpoint", "flink")
                .setTimestamp(timestamp, WritePrecision.MS);

        writeElementsOf(Collections.singletonList(point));
        LOG.debug("Checkpoint data point write at {}", point.toLineProtocol());
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Preparing to write the elements in InfluxDB.");
        this.writeCurrentElements();

        LOG.debug("Closing the writer.");
        this.influxDBClient.close();
    }

    private void writeCurrentElements() {
        writeElementsOf(this.elements);
        this.elements.clear();
    }

    private void writeElementsOf(List<Point> toWrite) {
        if (toWrite.isEmpty())
            return;

        this.influxDBClient.writePoints(toWrite);
        LOG.debug("Wrote {} data points", toWrite.size());
    }
}
