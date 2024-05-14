package com.sdg.ingestion.workers.pipeline.sinks;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Map;
import java.util.concurrent.TimeoutException;

public abstract class SinkFactory {

    private static final SinkFactoryBuilder builder = new SinkFactoryBuilder();

    public static SinkFactoryBuilder builder() {
        return builder;
    }

    public abstract Dataset<Row> read();

    public abstract Dataset transformation(Dataset dataset) throws TimeoutException, StreamingQueryException;

    public abstract Map<String, Dataset> run(Map<String, Dataset> datasetMap);
}
