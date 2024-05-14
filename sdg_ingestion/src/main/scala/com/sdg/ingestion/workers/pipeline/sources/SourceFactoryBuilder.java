package com.sdg.ingestion.workers.pipeline.sources;

import com.sdg.ingestion.config.Constants;
import com.sdg.ingestion.config.IngestionSettingsParameters;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import com.sdg.ingestion.workers.pipeline.sources.impl.SourceJSONFlow;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SourceFactoryBuilder {

    private Source source;
    private static final Logger LOG = LoggerFactory.getLogger(SourceFactoryBuilder.class);

    public SourceFactoryBuilder() {
        /* nothing to do */
    }

    public SourceFactoryBuilder source(Source source) {
        this.source = source;
        return this;
    }

    public SourceFactory builder(SparkSession session, String hdfsPrefix) throws Exception {
        switch (source.getFormat()) {
            case Constants.JSON_SOURCE_TYPE:
                return new SourceJSONFlow(session, source, hdfsPrefix);
            default:
                LOG.error("-----------------> Flow {} not allowed", this.source.getFormat());
                throw new Exception(String.format("Flow %s not allowed", this.source.getFormat()));
        }
    }
}
