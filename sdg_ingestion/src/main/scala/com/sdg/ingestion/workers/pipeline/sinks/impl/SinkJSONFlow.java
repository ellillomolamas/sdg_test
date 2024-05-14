package com.sdg.ingestion.workers.pipeline.sinks.impl;

import com.sdg.ingestion.config.Constants;
import com.sdg.ingestion.config.IngestionSettingsParameters;
import com.sdg.ingestion.config.dataflowSettings.sink.Sink;
import com.sdg.ingestion.workers.pipeline.sinks.SinkFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SinkJSONFlow extends SinkFactory {
    private String hdfsPrefix;
    private final Sink sink;
    private static SparkSession spark;

    private static final Logger LOG = LoggerFactory.getLogger(SinkJSONFlow.class);

    public SinkJSONFlow(SparkSession spark, Sink transformation, String hdfsPrefix) {
        this.sink = transformation;
        this.hdfsPrefix = hdfsPrefix;
        this.spark = spark;
    }

    public Dataset read() { return null; }

    public Dataset transformation(Dataset dataset){
        for(int outputPathsCounter = 0; outputPathsCounter< sink.getPaths().size(); outputPathsCounter++){
            String currentOutputPath = sink.getPaths().get(outputPathsCounter);
            SaveMode saveMode = null;
            switch (sink.getSaveMode()){
                case Constants.APPEND_SAVEMODE:
                    saveMode = SaveMode.Append;
                default:
                    saveMode = SaveMode.Overwrite;
            }
            dataset.coalesce(1).write().mode(saveMode).json(
                    hdfsPrefix + currentOutputPath + "/" + sink.getName()
            );
        }
        return dataset;
    }

    public Map<String, Dataset> run(Map<String, Dataset> datasetMap) {
        LOG.info("-----------------> Running SinkJSONFlow");
        //get input dataset
        Dataset nDataset = datasetMap.get(sink.getInput());
        nDataset.show(false);
        if (nDataset == null) {
            nDataset = this.read();
        }
        this.transformation(nDataset);
        return datasetMap;
    }
}
