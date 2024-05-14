package com.sdg.ingestion.workers.pipeline.transformations.impl;

import com.sdg.ingestion.config.Constants;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;
import com.sdg.ingestion.config.dataflowSettings.transformation.Validation;
import com.sdg.ingestion.workers.pipeline.transformations.TransformationFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransformationValidateFieldsFlow extends TransformationFactory {
    private final Transformation transformation;
    private static SparkSession spark;

    private static final Logger LOG = LoggerFactory.getLogger(TransformationValidateFieldsFlow.class);

    public TransformationValidateFieldsFlow(SparkSession spark, Transformation transformation) {
        this.transformation = transformation;
        this.spark = spark;
    }

    private String addFilterCondition(String input){
        switch (input) {
            case Constants.NOT_EMPTY_STRING_CONDITION:
                return Constants.NOT_EMPTY_VALUE_CONDITION;
            case Constants.NOT_NULL_STRING_CONDITION:
                return Constants.NOT_NULL_VALUE_CONDITION;
            default:
                return "";
        }
    }

    public Dataset read() { return null; }

    public Dataset transformation(Dataset dataset){
        String filter = "";
        for(int validationCounter=0; validationCounter < transformation.getParams().getValidations().size(); validationCounter++){
            Validation currentValidation = transformation.getParams().getValidations().get(validationCounter);
            for(int conditionsCounter=0; conditionsCounter < currentValidation.getValidations().size(); conditionsCounter++) {
                String currentCondition = currentValidation.getValidations().get(conditionsCounter);
                filter = filter + currentValidation.getField() + addFilterCondition(currentCondition);
                if (conditionsCounter <= currentValidation.getValidations().size()-1) {
                    filter = filter + " AND ";
                }
            }
            dataset = dataset.filter(filter);
        }
        return dataset;
    }

    public Map<String, Dataset> run(Map<String, Dataset> datasetMap) {
        LOG.info("-----------------> Running TransformationValidateFieldsFlow");
        //get input dataset
        Dataset inDataset = datasetMap.get(transformation.getParams().getInput());
        if (inDataset == null) {
            inDataset = this.read();
        }
        Dataset outDataset = this.transformation(inDataset);
        datasetMap.put(transformation.getName() + "_ok", outDataset);
        datasetMap.put(
            transformation.getName() + "_ko",
            inDataset.except(outDataset).withColumn("arraycoderrorbyfield", org.apache.spark.sql.functions.lit("{...}?????"))
        );
        return datasetMap;
    }
}
