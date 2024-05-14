package com.sdg.ingestion.config.dataflowSettings.transformation;


import java.io.Serializable;
import java.util.List;

public class TransformationParameters implements Serializable {
    private String input;
    private List<Validation> validations;
    private List<AddFields> addFields;

    public TransformationParameters(){}

    public TransformationParameters(
            String input,
            List<Validation> validations,
            List<AddFields> addFields
    ) {
        this.input = input;
        this.validations = validations;
        this.addFields = addFields;
    }

    public String getInput() {
        return input;
    }

    public void setInput(String input) {
        this.input = input;
    }

    public List<Validation> getValidations() {
        return validations;
    }

    public void setValidations(List<Validation> validations) {
        this.validations = validations;
    }

    public List<AddFields> getAddFields() {
        return addFields;
    }

    public void setAddFields(List<AddFields> addFields) {
        this.addFields = addFields;
    }
}
