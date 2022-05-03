/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialParser.extractValueAsString;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.common.ParseField;
import org.opensearch.common.Strings;

/**
 * UploadGeoJSONRequestContent is the Data model for UploadGeoJSONRequest's body
 */
public final class UploadGeoJSONRequestContent {

    public static final String GEOSPATIAL_DEFAULT_FIELD_NAME = "location";
    public static final ParseField FIELD_INDEX = new ParseField("index");
    public static final ParseField FIELD_GEOSPATIAL = new ParseField("field");
    public static final ParseField FIELD_GEOSPATIAL_TYPE = new ParseField("type");
    public static final ParseField FIELD_DATA = new ParseField("data");
    private final String indexName;
    private final String fieldName;
    private final String fieldType;
    private final List<Object> data;

    private UploadGeoJSONRequestContent(String indexName, String fieldName, String fieldType, List<Object> data) {
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
            fieldName = GEOSPATIAL_DEFAULT_FIELD_NAME; // use default filed name, if field name is empty
        }
        String fieldType = extractValueAsString(input, FIELD_GEOSPATIAL_TYPE.getPreferredName());
        if (!Strings.hasText(fieldType)) {
            throw new IllegalArgumentException("field [ " + FIELD_GEOSPATIAL_TYPE.getPreferredName() + " ] cannot be empty");
        }
        Object geoJSONData = Objects.requireNonNull(
            input.get(FIELD_DATA.getPreferredName()),
            "field [ " + FIELD_DATA.getPreferredName() + " ] cannot be empty"
        );
        if (!(geoJSONData instanceof List)) {
            throw new IllegalArgumentException(
                geoJSONData + " is not an instance of List, but of type [ " + geoJSONData.getClass().getName() + " ]"
            );
        }
        return new UploadGeoJSONRequestContent(index, fieldName, fieldType, (List<Object>) geoJSONData);
    }

    public String getIndexName() {
        return indexName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public List<Object> getData() {
        return data;
    }

    public String getFieldType() {
        return fieldType;
    }
}
