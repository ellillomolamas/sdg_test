package com.sdg.ingestion.workers.pipeline;

import com.sdg.ingestion.config.dataflowSettings.Dataflows;
import com.sdg.ingestion.config.dataflowSettings.sink.Sink;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;
import com.sdg.ingestion.workers.pipeline.sinks.SinkApi;
import com.sdg.ingestion.workers.pipeline.sources.SourceApi;
import com.sdg.ingestion.workers.pipeline.transformations.TransformationApi;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class ETLTool {

    public static void process(SparkSession spark, Dataflows dataflows, String hdfsPrefix) throws Exception {
        SourceApi sourceApi = new SourceApi(spark);
        TransformationApi transformationApi = new TransformationApi(spark);
        SinkApi sinkApi = new SinkApi(spark);

        Map<String, Dataset> datasetMap = new HashMap<String, Dataset>();
        for(int dataFlowsIndex=0; dataFlowsIndex < dataflows.getDataflows().size(); dataFlowsIndex++){
            for(int sourcesIndex=0; sourcesIndex < dataflows.getDataflows().get(dataFlowsIndex).getSources().size(); sourcesIndex++){
                Source currentSource = dataflows.getDataflows().get(dataFlowsIndex).getSources().get(sourcesIndex);
                datasetMap = sourceApi.apply(datasetMap, currentSource, hdfsPrefix);
            }
            datasetMap.get("person_inputs").show(false);
            for(int transformationIndex=0; transformationIndex < dataflows.getDataflows().get(dataFlowsIndex).getTransformations().size(); transformationIndex++){
                Transformation currentTransformation = dataflows.getDataflows().get(dataFlowsIndex).getTransformations().get(transformationIndex);
                datasetMap = transformationApi.apply(datasetMap, currentTransformation);
            }
            datasetMap.get("validation_ok").show(false);
            datasetMap.get("validation_ko").show(false);
            datasetMap.get("ok_with_date").show(false);

            for(int sinkIndex=0; sinkIndex < dataflows.getDataflows().get(dataFlowsIndex).getSinks().size(); sinkIndex++){
                Sink currentSink = dataflows.getDataflows().get(dataFlowsIndex).getSinks().get(sinkIndex);
                datasetMap = sinkApi.apply(datasetMap, currentSink, hdfsPrefix);
            }
        }
    }
}
