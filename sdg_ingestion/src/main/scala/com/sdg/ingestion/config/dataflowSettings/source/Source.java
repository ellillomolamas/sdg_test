package com.sdg.ingestion.config.dataflowSettings.source;

import java.io.Serializable;

public class Source implements Serializable {
    private String name;
    private String path;
    private String format;

    public Source(){}

    public Source(String name) {
        this.name = name;
    }

    public Source(String name, String path, String format) {
        this.name = name;
        this.path = path;
        this.format = format;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
