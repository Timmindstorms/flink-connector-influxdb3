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
 * - Added error handling (try-catch) to the close method.
 * - Updated imports to reflex new package structure.
 * - Updated package to dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.commiter.
 */
package dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.commiter;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.write.WritePrecision;
import com.influxdb.v3.client.Point;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.InfluxDBSinkOptions.WRITE_DATA_POINT_CHECKPOINT;
import static dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.InfluxDBSinkOptions.getInfluxDBClient;

/**
 * The InfluxDBCommitter implements the {@link Committer} interface The
 * InfluxDBCommitter is called
 * whenever a checkpoint is set by Flink. When this class is called it writes a
 * checkpoint data
 * point in InfluxDB. The checkpoint data point uses the latest written record
 * timestamp.
 */
@Internal
public final class InfluxDBCommitter implements Committer<Long> {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBCommitter.class);

    private final InfluxDBClient influxDBClient;
    private final boolean writeCheckpoint;

    public InfluxDBCommitter(final Configuration configuration) {
        this.influxDBClient = getInfluxDBClient(configuration);
        this.writeCheckpoint = configuration.getBoolean(WRITE_DATA_POINT_CHECKPOINT);
    }

    /**
     * This method is called only when a checkpoint is set and writes a checkpoint
     * data point into
     * InfluxDB. The {@link
     * dev.timmindstorms.flink.streaming.connectors.influxdb3.sink.writer.InfluxDBWriter}
     * prepares the
     * commit and fills the commitable list with the latest timestamp. If the list
     * contains a single
     * element it will be used as the timestamp of the datapoint. Otherwise when no
     * timestamp is
     * provided, InfluxDB will use the current timestamp (UTC) of the host machine.
     *
     * <p>
     *
     * @param committables Contains the latest written timestamp.
     * @return Empty list
     * @see <a
     *      href=https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/#timestamp></a>
     */
    @Override
    public List<Long> commit(final List<Long> committables) {
        if (this.writeCheckpoint) {
            LOG.debug("A checkpoint is set.");
            Optional<Long> lastTimestamp = Optional.empty();
            if (committables.size() >= 1) {
                lastTimestamp = Optional.ofNullable(committables.get(committables.size() - 1));
            }
            this.writeCheckpointDataPoint(lastTimestamp);
        }
        return Collections.emptyList();
    }

    @Override
    public void close() {
        try {
            this.influxDBClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        LOG.debug("Closing the committer.");
    }

    private void writeCheckpointDataPoint(final Optional<Long> timestamp) {
        final Point point = new Point("checkpoint");
        point.setField("checkpoint", "flink");
        timestamp.ifPresent(aTime -> point.setTimestamp(aTime, WritePrecision.MS));
        this.influxDBClient.writePoint(point);
        LOG.debug("Checkpoint data point write at {}", point.toLineProtocol());
    }
}
