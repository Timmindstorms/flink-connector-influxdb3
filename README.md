# InfluxDB 3 Connector for Apache Flink

> **Disclaimer:** This is an unofficial community project. It is not affiliated with, maintained by, or endorsed by the Apache Software Foundation or InfluxData.

A Flink connector for writing data using the [streaming api](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/connectors/datastream/overview/) to [InfluxDB 3](https://www.influxdata.com/). This connector is a derivative work based on the [Apache Bahir InfluxDB 2 connector](https://github.com/apache/bahir-flink/tree/master/flink-connector-influxdb2), updated to support the InfluxDB v3 Java Client. This connector is tested with flink version 1.18.

## Installation

Install the dependency by running:

```bash
mvn clean install
```

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>dev.timmindstorms</groupId>
    <artifactId>flink-connector-influxdb3</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Usage

For more info on how to use it and the internal workings refer to the [original readme](https://github.com/apache/bahir-flink/blob/master/flink-connector-influxdb2/README.md).

### Sink

The Sink writes data points to InfluxDB using the [InfluxDB3 Java Client](https://github.com/InfluxCommunity/influxdb3-java). You provide the connection information (URL, token, database) and an implementation of `InfluxDBSchemaSerializer<IN>` generic interface.

```java
// You can use the build pattern to create a Sink object
InfluxDBSink<Measurement> influxDBSink = InfluxDBSink.builder()
        .setInfluxDBSchemaSerializer(new TestSerializer())
        .setInfluxDBUrl(getUrl())
        .setInfluxDBDatabase(getDatabase())
        .setInfluxDBToken(getToken())
        .build();

// Implementation of InfluxDBSchemaSerializer interface
public class TestSerializer implements InfluxDBSchemaSerializer<SensorMeasurement> {

    @Override
    public Point serialize(Measurement measurement, Context context) {
        return Point.measurement("test")
                .setTag("station", measurement.station)
                .setField("x", measurement.x)
                .setField("y", measurement.y)
                .setField("z", measurement.z)
                .setTimestamp(measurement.timestamp, WritePrecision.MS);
    }
}
```

#### Options

| Method                            | Description                                    | Required | Default |
| :-------------------------------- | :--------------------------------------------- | :------- | :------ |
| `setInfluxDBUrl(String)`          | The URL of your InfluxDB 3 instance.           | Yes      | -       |
| `setInfluxDBToken(String)`        | The API token for authentication.              | Yes      | -       |
| `setInfluxDBDatabase(String)`     | The target database name.                      | Yes      | -       |
| `setWriteBufferSize(int)`         | Number of points to buffer before writing.     | No       | 1000    |
| `addCheckpointDataPoint(boolean)` | Write a debug point on every Flink checkpoint. | No       | `false` |

## Building from Source

The connector can be build using maven:

```bash
mvn clean package
```

## License and Attribution

**License:** [Apache License 2.0](https://www.google.com/search?q=LICENSE)

This project is a derivative work of the **[Apache Bahir InfluxDB 2 connector](https://github.com/apache/bahir-flink/tree/master/flink-connector-influxdb2)**.

- **Original Project:** [Apache Bahir](https://bahir.apache.org/)
- **Original Copyright:** Copyright (c) 2016-2017 The Apache Software Foundation.
- **Modifications:** Copyright (c) 2025 timmindstorms.

See the `NOTICE` file for full attribution details.
