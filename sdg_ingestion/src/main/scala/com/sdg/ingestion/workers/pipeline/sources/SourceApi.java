package com.sdg.ingestion.workers.pipeline.sources;

import com.sdg.ingestion.config.IngestionSettingsParameters;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SourceApi {

    private static final Logger LOG = LoggerFactory.getLogger(SourceApi.class);
    private static SparkSession spark;

    public SourceApi(SparkSession spark) {
        this.spark = spark;
    }

    public Map<String, Dataset> apply(Map<String, Dataset> map, Source source, String hdfsPrefix) throws Exception {
        try {
            SourceFactory sourceFactory = SourceFactory.builder().source(source).builder(spark, hdfsPrefix);
            return sourceFactory.run(map);
        } catch (Exception e) {
            throw new Exception(e.getMessage(), e.getCause());
        }
    }

}
