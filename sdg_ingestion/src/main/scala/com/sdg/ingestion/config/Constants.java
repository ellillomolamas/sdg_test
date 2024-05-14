package com.sdg.ingestion.config;

public class Constants {
    public static final String JSON_SOURCE_TYPE = "JSON";
    public static final String KAFKA_TYPE = "KAFKA";

    public static final String VALIDATE_FIELDS_TRANSFORMATION_TYPE = "validate_fields";
    public static final String ADD_FIELDS_TRANSFORMATION_TYPE = "add_fields";

    public static final String NOT_EMPTY_STRING_CONDITION = "notEmpty";
    public static final String NOT_NULL_STRING_CONDITION = "notNull";
    public static final String NOT_EMPTY_VALUE_CONDITION = " <> \"\"";
    public static final String NOT_NULL_VALUE_CONDITION = " IS NOT NULL";

    public static final String CURRENT_TIMESTAMP_FUNCTION = "current_timestamp";

    public static final String OVERWRITE_SAVEMODE = "OVERWRITE";
    public static final String APPEND_SAVEMODE = "APPEND";

}
