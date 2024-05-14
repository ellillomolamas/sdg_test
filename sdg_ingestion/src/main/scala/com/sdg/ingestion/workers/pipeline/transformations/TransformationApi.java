package com.sdg.ingestion.workers.pipeline.transformations;

import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TransformationApi {

    private static final Logger LOG = LoggerFactory.getLogger(TransformationApi.class);
    private SparkSession spark;

    public TransformationApi(SparkSession spark) {
        this.spark = spark;
    }

    public Map<String, Dataset> apply(Map<String, Dataset> map, Transformation transformation) throws Exception {
        try {
            TransformationFactory transformationFactory = TransformationFactory.builder().transformation(transformation).builder(spark);
            return transformationFactory.run(map);
        } catch (Exception e) {
            throw new Exception(e.getMessage(), e.getCause());
        }
    }

}
