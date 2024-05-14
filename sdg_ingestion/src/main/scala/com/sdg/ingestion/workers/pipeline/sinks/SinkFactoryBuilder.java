package com.sdg.ingestion.workers.pipeline.sinks;

import com.sdg.ingestion.config.Constants;
import com.sdg.ingestion.config.IngestionSettingsParameters;
import com.sdg.ingestion.config.dataflowSettings.sink.Sink;
import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;
import com.sdg.ingestion.workers.pipeline.sinks.impl.SinkJSONFlow;
import com.sdg.ingestion.workers.pipeline.sinks.impl.SinkKafkaFlow;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SinkFactoryBuilder {
    private Sink sink;
    private static final Logger LOG = LoggerFactory.getLogger(SinkFactoryBuilder.class);

    public SinkFactoryBuilder() {
        /* nothing to do */
    }

    public SinkFactoryBuilder sink(Sink sink) {
        this.sink = sink;
        return this;
    }

    public SinkFactory builder(SparkSession session, String hdfsPrefix) throws Exception {
        switch (sink.getFormat()) {
            case Constants.JSON_SOURCE_TYPE:
                return new SinkJSONFlow(session, sink, hdfsPrefix);
            case Constants.KAFKA_TYPE:
                return new SinkKafkaFlow(session, sink);
            default:
                LOG.error("-----------------> Flow {} not allowed", this.sink.getFormat());
                throw new Exception(String.format("Flow %s not allowed", this.sink.getFormat()));
        }
    }
}
