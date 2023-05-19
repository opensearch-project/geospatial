/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialParser.extractValueAsString;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.geospatial.GeospatialParser;

/**
 * UploadGeoJSONRequestContent is the Data model for UploadGeoJSONRequest's body
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class UploadGeoJSONRequestContent {

    public static final String GEOSPATIAL_DEFAULT_FIELD_NAME = "location";
    public static final ParseField FIELD_INDEX = new ParseField("index");
    public static final ParseField FIELD_GEOSPATIAL = new ParseField("field");
    public static final ParseField FIELD_GEOSPATIAL_TYPE = new ParseField("type");
    public static final ParseField FIELD_DATA = new ParseField("data");

    // Custom Vector Map can support fetching up to 10K Features. Hence, we chose same value as limit
    // for upload as well.
    public static final int MAX_SUPPORTED_GEOJSON_FEATURE_COUNT = 10_000;
    private final String indexName;
    private final String fieldName;
    private final String fieldType;
    private final List<Object> data;

    /**
     * Creates UploadGeoJSONRequestContent from the user input
     * @param input user input of type Map
     * @return UploadGeoJSONRequestContent based on value from input
     * @throws NullPointerException if input is null
     * @throws IllegalArgumentException if input doesn't have valid arguments
     */
    public static UploadGeoJSONRequestContent create(Map<String, Object> input) {
        Objects.requireNonNull(input, "input cannot be null");
        final String index = validateIndexName(input);
        String fieldName = extractValueAsString(input, FIELD_GEOSPATIAL.getPreferredName());
        if (!Strings.hasText(fieldName)) {
            fieldName = GEOSPATIAL_DEFAULT_FIELD_NAME; // use default filed name, if field name is empty
        }
        final String fieldType = extractValueAsString(input, FIELD_GEOSPATIAL_TYPE.getPreferredName());
        if (!Strings.hasText(fieldType)) {
            throw new IllegalArgumentException("field [ " + FIELD_GEOSPATIAL_TYPE.getPreferredName() + " ] cannot be empty");
        }
        final Object geoJSONData = Objects.requireNonNull(
            input.get(FIELD_DATA.getPreferredName()),
            "field [ " + FIELD_DATA.getPreferredName() + " ] cannot be empty"
        );
        if (!(geoJSONData instanceof List)) {
            throw new IllegalArgumentException(
                geoJSONData + " is not an instance of List, but of type [ " + geoJSONData.getClass().getName() + " ]"
            );
        }
        validateFeatureCount(geoJSONData);
        return new UploadGeoJSONRequestContent(index, fieldName, fieldType, (List<Object>) geoJSONData);
    }

    private static void validateFeatureCount(Object geoJSONData) {
        final long featureCount = getFeatureCount(geoJSONData);
        if (featureCount > MAX_SUPPORTED_GEOJSON_FEATURE_COUNT) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "Received %d features, but, cannot upload more than %d features",
                    featureCount,
                    MAX_SUPPORTED_GEOJSON_FEATURE_COUNT
                )
            );
        }
    }

    private static long getFeatureCount(Object geoJSONData) {
        return ((List<Object>) geoJSONData).stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(GeospatialParser::getFeatures)
            .flatMap(List::stream)
            .count();
    }

    private static String validateIndexName(Map<String, Object> input) {
        String index = extractValueAsString(input, FIELD_INDEX.getPreferredName());
        if (Strings.hasText(index)) {
            return index;
        }
        throw new IllegalArgumentException(
            String.format(Locale.getDefault(), "field [ %s ] cannot be empty", FIELD_INDEX.getPreferredName())
        );
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
