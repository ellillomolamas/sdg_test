package com.sdg.ingestion.workers.pipeline.sinks;

import com.sdg.ingestion.config.IngestionSettingsParameters;
import com.sdg.ingestion.config.dataflowSettings.sink.Sink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkApi {

    private static final Logger LOG = LoggerFactory.getLogger(SinkApi.class);
    private SparkSession spark;

    public SinkApi(SparkSession spark) {
        this.spark = spark;
    }

    public Map<String, Dataset> apply(Map<String, Dataset> map, Sink sink, String hdfsPrefix) throws Exception {
        try {
            SinkFactory sinkFactory = SinkFactory.builder().sink(sink).builder(spark, hdfsPrefix);
            return sinkFactory.run(map);
        } catch (Exception e) {
            throw new Exception(e.getMessage(), e.getCause());
        }
    }

}
