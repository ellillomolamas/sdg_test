package com.sdg.ingestion.config.dataflowSettings.transformation;

import java.io.Serializable;
import java.util.List;

public class Validation implements Serializable {
    private String field;
    private List<String> validations;

    public Validation(){}

    public Validation(String field, List<String> validations) {
        this.field = field;
        this.validations = validations;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public List<String> getValidations() {
        return validations;
    }

    public void setValidations(List<String> validations) {
        this.validations = validations;
    }
}
