package com.sdg.ingestion.workers.pipeline.transformations;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

public abstract class TransformationFactory {

    private static final TransformationFactoryBuilder builder = new TransformationFactoryBuilder();

    public static TransformationFactoryBuilder builder() {
        return builder;
    }

    private final Date nowDate = new Date();

    private final String transactionDate = new SimpleDateFormat("yyyy-MM-dd").format(nowDate);

    public abstract Dataset<Row> read();

    public abstract Dataset transformation(Dataset dataset);

    public abstract Map<String, Dataset> run(Map<String, Dataset> datasetMap);
}
