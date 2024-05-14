package com.sdg.ingestion.config.dataflowSettings.transformation;

import java.io.Serializable;

public class Transformation implements Serializable {
    private String name;
    private String type;
    private TransformationParameters params;

    public Transformation(){}

    public Transformation(String name, String type, TransformationParameters params) {
        this.name = name;
        this.type = type;
        this.params = params;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public TransformationParameters getParams() {
        return params;
    }

    public void setParams(TransformationParameters params) {
        this.params = params;
    }
}
