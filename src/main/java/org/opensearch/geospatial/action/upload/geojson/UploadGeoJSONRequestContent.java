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
    public static final ParseField FIELD_DATA = new ParseField("data", new String[0]);
    private final String indexName;
    private final String geospatialFieldName;
    private final Object data;

    private UploadGeoJSONRequestContent(String indexName, String geospatialFieldName, Object data) {
        this.indexName = indexName;
        this.geospatialFieldName = geospatialFieldName;
        this.data = data;
    }

    public static UploadGeoJSONRequestContent create(Map<String, Object> input) {
        Objects.requireNonNull(input, "input cannot be null");
        String index = extractValueAsString(input, FIELD_INDEX.getPreferredName());
        if (!Strings.hasText(index)) {
            throw new IllegalArgumentException("field [ " + FIELD_INDEX.getPreferredName() + " ] cannot be empty");
        }
        String geospatialField = extractValueAsString(input, FIELD_GEOSPATIAL.getPreferredName());
        if (!Strings.hasText(geospatialField)) {
            throw new IllegalArgumentException("field [ " + FIELD_GEOSPATIAL.getPreferredName() + " ] cannot be empty");
        }
        Object geoJSONData = Objects.requireNonNull(
            input.get(FIELD_DATA.getPreferredName()),
            "field [ " + FIELD_DATA.getPreferredName() + " ] cannot be empty"
        );
        return new UploadGeoJSONRequestContent(index, geospatialField, geoJSONData);
    }

    public final String getIndexName() {
        return indexName;
    }

    public final String getGeospatialFieldName() {
        return geospatialFieldName;
    }

    public Object getData() {
        return data;
    }
}
