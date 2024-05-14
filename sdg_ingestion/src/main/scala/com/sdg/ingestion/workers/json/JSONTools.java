package com.sdg.ingestion.workers.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sdg.ingestion.config.dataflowSettings.Dataflows;

import java.io.File;
import java.io.IOException;

public class JSONTools {

    public static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public static Dataflows readJSON(String path) throws IOException {
        File myFile = new File(path);
        Dataflows d = JSON_MAPPER.readValue(myFile, Dataflows.class);
        return d;
    }

    public static String printJSON(Dataflows dataflows) throws JsonProcessingException {
       return JSON_MAPPER
                .writerWithDefaultPrettyPrinter() // enable pretty print
                .writeValueAsString(dataflows);
    }
}
