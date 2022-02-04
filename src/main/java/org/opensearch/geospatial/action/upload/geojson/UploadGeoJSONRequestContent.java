/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialParser.extractValueAsString;

import java.util.Map;
import java.util.Objects;

import org.opensearch.common.ParseField;
import org.opensearch.common.Strings;

/**
 * UploadGeoJSONRequestContent is the Data model for UploadGeoJSONRequest's body
 */
public final class UploadGeoJSONRequestContent {

    public static final ParseField FIELD_INDEX = new ParseField("index", new String[0]);
    public static final ParseField FIELD_GEOSPATIAL = new ParseField("field", new String[0]);
    public static final ParseField FIELD_GEOSPATIAL_TYPE = new ParseField("type", new String[0]);
    public static final ParseField FIELD_DATA = new ParseField("data", new String[0]);
    private final String indexName;
    private final String fieldName;
    private final String fieldType;
    private final Object data;

    private UploadGeoJSONRequestContent(String indexName, String fieldName, String fieldType, Object data) {
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.data = data;
    }

    /**
     * Creates UploadGeoJSONRequestContent from the user input
     * @param input user input of type Map
     * @return UploadGeoJSONRequestContent based on value from input
     * @throws NullPointerException if input is null
     * @throws IllegalArgumentException if input doesn't have valid arguments
     */
    public static UploadGeoJSONRequestContent create(Map<String, Object> input) {
        Objects.requireNonNull(input, "input cannot be null");
        String index = extractValueAsString(input, FIELD_INDEX.getPreferredName());
        if (!Strings.hasText(index)) {
            throw new IllegalArgumentException("field [ " + FIELD_INDEX.getPreferredName() + " ] cannot be empty");
        }
        String fieldName = extractValueAsString(input, FIELD_GEOSPATIAL.getPreferredName());
        if (!Strings.hasText(fieldName)) {
            throw new IllegalArgumentException("field [ " + FIELD_GEOSPATIAL.getPreferredName() + " ] cannot be empty");
        }
        String fieldType = extractValueAsString(input, FIELD_GEOSPATIAL_TYPE.getPreferredName());
        if (!Strings.hasText(fieldType)) {
            throw new IllegalArgumentException("field [ " + FIELD_GEOSPATIAL_TYPE.getPreferredName() + " ] cannot be empty");
        }
        Object geoJSONData = Objects.requireNonNull(
            input.get(FIELD_DATA.getPreferredName()),
            "field [ " + FIELD_DATA.getPreferredName() + " ] cannot be empty"
        );
        return new UploadGeoJSONRequestContent(index, fieldName, fieldType, geoJSONData);
    }

    public final String getIndexName() {
        return indexName;
    }

    public final String getFieldName() {
        return fieldName;
    }

    public Object getData() {
        return data;
    }

    public String getFieldType() {
        return fieldType;
    }
}
