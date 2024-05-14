package com.sdg.ingestion.config.dataflowSettings.sink;

import java.io.Serializable;
import java.util.List;

public class Sink implements Serializable {
    private String input;
    private String name;
    private List<String> topics;
    private List<String> paths;
    private String format;
    private String saveMode;

    public Sink(){}

    public Sink(
            String input,
            String name,
            List<String> topics,
            List<String> paths,
            String format,
            String saveMode
    ) {
        this.input = input;
        this.name = name;
        this.topics = topics;
        this.paths = paths;
        this.format = format;
        this.saveMode = saveMode;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<String> getPaths() {
        return paths;
    }

    public void setPaths(List<String> paths) {
        this.paths = paths;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getSaveMode() {
        return saveMode;
    }

    public void setSaveMode(String saveMode) {
        this.saveMode = saveMode;
    }
}
