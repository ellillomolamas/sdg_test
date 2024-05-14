package com.sdg.ingestion.workers.pipeline.transformations;

import com.sdg.ingestion.config.Constants;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;
import com.sdg.ingestion.workers.pipeline.sources.impl.SourceJSONFlow;
import com.sdg.ingestion.workers.pipeline.transformations.impl.TransformationAddFieldsFlow;
import com.sdg.ingestion.workers.pipeline.transformations.impl.TransformationValidateFieldsFlow;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransformationFactoryBuilder {

    private Transformation transformation;
    private static final Logger LOG = LoggerFactory.getLogger(TransformationFactoryBuilder.class);

    public TransformationFactoryBuilder() {
        /* nothing to do */
    }

    public TransformationFactoryBuilder transformation(Transformation transformation) {
        this.transformation = transformation;
        return this;
    }

    public TransformationFactory builder(SparkSession session) throws Exception {
        switch (transformation.getType()) {
            case Constants.VALIDATE_FIELDS_TRANSFORMATION_TYPE:
                return new TransformationValidateFieldsFlow(session, transformation);
            case Constants.ADD_FIELDS_TRANSFORMATION_TYPE:
                return new TransformationAddFieldsFlow(session, transformation);
            default:
                LOG.error("-----------------> Flow {} not allowed", this.transformation.getType());
                throw new Exception(String.format("Flow %s not allowed", this.transformation.getType()));
        }
    }
}
