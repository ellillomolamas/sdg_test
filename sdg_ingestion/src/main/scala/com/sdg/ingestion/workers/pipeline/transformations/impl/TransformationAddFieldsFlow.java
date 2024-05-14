package com.sdg.ingestion.workers.pipeline.transformations.impl;

import com.sdg.ingestion.config.Constants;
import com.sdg.ingestion.config.dataflowSettings.transformation.AddFields;
import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;
import com.sdg.ingestion.workers.pipeline.transformations.TransformationFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransformationAddFieldsFlow extends TransformationFactory {
    private final Transformation transformation;
    private static SparkSession spark;

    private static final Logger LOG = LoggerFactory.getLogger(TransformationAddFieldsFlow.class);

    public TransformationAddFieldsFlow(SparkSession spark, Transformation transformation) {
        this.transformation = transformation;
        this.spark = spark;
    }

    public Dataset read() { return null; }

    public Dataset transformation(Dataset dataset){
        for(int fieldsToAddCounter=0; fieldsToAddCounter<transformation.getParams().getAddFields().size(); fieldsToAddCounter++){
            AddFields currentFieldToAdd = transformation.getParams().getAddFields().get(fieldsToAddCounter);
            switch (currentFieldToAdd.getFunction()) {
                case Constants.CURRENT_TIMESTAMP_FUNCTION:
                    dataset = dataset.withColumn(currentFieldToAdd.getName(), org.apache.spark.sql.functions.current_timestamp());
                default:
                    dataset = dataset;
            }
        }
        return dataset;
    }

    public Map<String, Dataset> run(Map<String, Dataset> datasetMap) {
        LOG.info("-----------------> Running TransformationAddFieldsFlow");
        //get input dataset
        Dataset nDataset = datasetMap.get(transformation.getParams().getInput());
        if (nDataset == null) {
            nDataset = this.read();
        }
        nDataset = this.transformation(nDataset);
        datasetMap.put(transformation.getName(), nDataset);
        return datasetMap;
    }
}
