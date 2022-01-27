/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static java.util.Objects.requireNonNull;
import static org.opensearch.geospatial.GeospatialParser.extractValueAsString;

import java.util.Map;

import org.opensearch.common.ParseField;

public final class UploadGeoJSONRequestContent {

    public static final ParseField FIELD_INDEX = new ParseField("index", new String[0]);
    public static final ParseField FIELD_GEOSPATIAL = new ParseField("field", new String[0]);
    public static final ParseField FIELD_DATA = new ParseField("data", new String[0]);
    private final String indexName;
    private final String geospatialFieldName;
    private final Object data;

    private UploadGeoJSONRequestContent(String indexName, String geospatialFieldName, Object data) {
        this.indexName = requireNonNull(indexName);
        this.geospatialFieldName = requireNonNull(geospatialFieldName);
        this.data = requireNonNull(data);
    }

    public static UploadGeoJSONRequestContent create(Map<String, Object> input) {
        String index = extractValueAsString(input, FIELD_INDEX.getPreferredName());
        String geospatialField = extractValueAsString(input, FIELD_GEOSPATIAL.getPreferredName());
        return new UploadGeoJSONRequestContent(index, geospatialField, input.get(FIELD_DATA.getPreferredName()));
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
