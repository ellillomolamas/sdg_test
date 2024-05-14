package com.sdg.ingestion.config.dataflowSettings.transformation;

import java.io.Serializable;

public class AddFields implements Serializable {
    private String name;
    private String function;

    public AddFields() {}

    public AddFields(String name, String function) {
        this.name = name;
        this.function = function;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }
}
