package com.sdg.ingestion.workers.pipeline.sources.impl;

import com.sdg.ingestion.config.IngestionSettingsParameters;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import com.sdg.ingestion.workers.pipeline.sources.SourceFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SourceJSONFlow extends SourceFactory {
    private final Source source;
    private String hdfsPrefix;
    private static SparkSession spark;

    private static final Logger LOG = LoggerFactory.getLogger(SourceJSONFlow.class);

    public SourceJSONFlow(Source source) {
        this.source = source;
    }

    public SourceJSONFlow(SparkSession spark, Source source, String hdfsPrefix) {
        this.source = source;
        this.hdfsPrefix = hdfsPrefix;
        this.spark = spark;
    }

    public Dataset read() {
        return spark.read().json(hdfsPrefix + source.getPath());
    }

    public Dataset transformation(Dataset dataset) {
        return dataset;
    }

    public Map<String, Dataset> run(Map<String, Dataset> datasetMap) {
        Dataset nDataset = datasetMap.get(source.getName());
        if (nDataset == null) {
            nDataset = this.read();
        }
        nDataset = this.transformation(nDataset);
        datasetMap.put(source.getName(), nDataset);
        return datasetMap;
    }
}
