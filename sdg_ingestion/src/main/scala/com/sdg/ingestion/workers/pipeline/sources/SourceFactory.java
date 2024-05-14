package com.sdg.ingestion.workers.pipeline.sources;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public abstract class SourceFactory {

    private static final SourceFactoryBuilder builder = new SourceFactoryBuilder();

    public static SourceFactoryBuilder builder() {
        return builder;
    }

    private final Date nowDate = new Date();

    private final String transactionDate = new SimpleDateFormat("yyyy-MM-dd").format(nowDate);

    public abstract Dataset<Row> read();

    public abstract Dataset transformation(Dataset dataset);

    public abstract Map<String, Dataset> run(Map<String, Dataset> datasetMap);
}
