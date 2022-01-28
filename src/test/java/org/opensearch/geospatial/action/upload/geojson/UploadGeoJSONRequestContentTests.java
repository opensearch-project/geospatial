/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.FIELD_DATA;

import java.util.Collections;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestContentTests extends OpenSearchTestCase {

    private Map<String, Object> buildRequestContent(String indexName, String fieldName) {
        JSONObject contents = new JSONObject();
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), indexName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), fieldName);
        JSONArray values = new JSONArray();
        values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents.toMap();
    }

    public void testCreate() {
        final String indexName = "some-index";
        final String fieldName = "location";
        Map<String, Object> contents = buildRequestContent(indexName, fieldName);
        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(contents);
        assertNotNull(content);
        assertEquals(fieldName, content.getGeospatialFieldName());
        assertEquals(indexName, content.getIndexName());
        assertEquals(contents.get(FIELD_DATA.getPreferredName()), content.getData());
    }
}
