package com.sdg.ingestion.config.dataflowSettings;

import java.io.Serializable;
import java.util.List;

public class Dataflows implements Serializable {
    private List<Dataflow> dataflows;

    public Dataflows(){}

    public Dataflows(List<Dataflow> dataflows) {
        this.dataflows = dataflows;
    }

    public List<Dataflow> getDataflows() {
        return dataflows;
    }

    public void setDataflows(List<Dataflow> dataflows) {
        this.dataflows = dataflows;
    }
}
