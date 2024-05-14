package com.sdg.ingestion.config.dataflowSettings;

import com.sdg.ingestion.config.dataflowSettings.sink.Sink;
import com.sdg.ingestion.config.dataflowSettings.source.Source;
import com.sdg.ingestion.config.dataflowSettings.transformation.Transformation;

import java.io.Serializable;
import java.util.List;

public class Dataflow implements Serializable {
    private String name;
    private List<Source> sources;
    private List<Transformation> transformations;
    private List<Sink> sinks;

    public Dataflow(){}

    public Dataflow(String name,
                    List<Source> sources,
                    List<Transformation> transformations,
                    List<Sink> sinks
    ) {
        this.name = name;
        this.sources = sources;
        this.transformations = transformations;
        this.sinks = sinks;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Source> getSources() {
        return sources;
    }

    public void setSources(List<Source> sources) {
        this.sources = sources;
    }

    public List<Transformation> getTransformations() {
        return transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        this.transformations = transformations;
    }

    public List<Sink> getSinks() {
        return sinks;
    }

    public void setSinks(List<Sink> sinks) {
        this.sinks = sinks;
    }
}
