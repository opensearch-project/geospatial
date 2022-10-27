/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialObjectBuilder.buildProperties;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.FIELD_DATA;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.GEOSPATIAL_DEFAULT_FIELD_NAME;

import java.util.Collections;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestContentTests extends OpenSearchTestCase {
    private String indexName;
    private String fieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        fieldName = randomLowerCaseString();
    }

    private Map<String, Object> buildRequestContent(String indexName, String fieldName) {
        final var contents = new JSONObject();
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), indexName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), fieldName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName(), "geo_shape");
        JSONArray values = new JSONArray();
        values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents.toMap();
    }

    public void testCreate() {
        Map<String, Object> contents = buildRequestContent(indexName, fieldName);
        final var content = UploadGeoJSONRequestContent.create(contents);
        assertNotNull(content);
        assertEquals(fieldName, content.getFieldName());
        assertEquals(indexName, content.getIndexName());
        assertEquals(contents.get(FIELD_DATA.getPreferredName()), content.getData());
    }

    public void testCreateEmptyIndexName() {
        IllegalArgumentException invalidIndexName = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(buildRequestContent("", "location"))
        );
        assertTrue(invalidIndexName.getMessage().contains("[ index ] cannot be empty"));
    }

    public void testCreateEmptyGeospatialFieldName() {
        final var content = UploadGeoJSONRequestContent.create(buildRequestContent(randomLowerCaseString(), ""));
        assertNotNull(content);
        assertEquals("wrong field name", GEOSPATIAL_DEFAULT_FIELD_NAME, content.getFieldName());
    }

    public void testCreateEmptyGeospatialFieldType() {
        Map<String, Object> contents = buildRequestContent(indexName, fieldName);
        contents.remove(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName());
        IllegalArgumentException invalidIndexName = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(contents)
        );
        assertTrue(invalidIndexName.getMessage().contains("[ type ] cannot be empty"));
    }
}
