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
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.MAX_SUPPORTED_GEOJSON_FEATURE_COUNT;

import java.util.Collections;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestContentTests extends OpenSearchTestCase {
    private static int MIN_FEATURE_COUNT = 3;
    private String indexName;
    private String fieldName;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexName = randomLowerCaseString();
        fieldName = randomLowerCaseString();
    }

    private Map<String, Object> buildRequestContent(String indexName, String fieldName, int count) {
        final var contents = new JSONObject();
        contents.put(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName(), indexName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL.getPreferredName(), fieldName);
        contents.put(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName(), "geo_shape");
        JSONArray values = new JSONArray();
        for (int i = 0; i < count; i++) {
            values.put(randomGeoJSONFeature(buildProperties(Collections.emptyMap())));
        }
        contents.put(FIELD_DATA.getPreferredName(), values);
        return contents.toMap();
    }

    public void testCreate() {
        Map<String, Object> contents = buildRequestContent(indexName, fieldName, MIN_FEATURE_COUNT);
        final var content = UploadGeoJSONRequestContent.create(contents);
        assertNotNull(content);
        assertEquals(fieldName, content.getFieldName());
        assertEquals(indexName, content.getIndexName());
        assertEquals(contents.get(FIELD_DATA.getPreferredName()), content.getData());
    }

    public void testCreateEmptyIndexName() {
        IllegalArgumentException invalidIndexName = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(buildRequestContent("", "location", MIN_FEATURE_COUNT))
        );
        assertTrue(invalidIndexName.getMessage().contains("[ index ] cannot be empty"));
    }

    public void testCreateWithOneMoreThanMaxSupportedFeatureCount() {
        int featureCount = MAX_SUPPORTED_GEOJSON_FEATURE_COUNT + 1;
        IllegalArgumentException reachedMaxFeatureCount = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(buildRequestContent(indexName, fieldName, featureCount))
        );
        assertEquals(
            "wrong error returned",
            reachedMaxFeatureCount.getMessage(),
            "Received 10001 features, but, cannot upload more than 10000 features"
        );
    }

    public void testCreateEmptyGeospatialFieldName() {
        final var content = UploadGeoJSONRequestContent.create(buildRequestContent(randomLowerCaseString(), "", MIN_FEATURE_COUNT));
        assertNotNull(content);
        assertEquals("wrong field name", GEOSPATIAL_DEFAULT_FIELD_NAME, content.getFieldName());
    }

    public void testCreateEmptyGeospatialFieldType() {
        Map<String, Object> contents = buildRequestContent(indexName, fieldName, MIN_FEATURE_COUNT);
        contents.remove(UploadGeoJSONRequestContent.FIELD_GEOSPATIAL_TYPE.getPreferredName());
        IllegalArgumentException invalidIndexName = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(contents)
        );
        assertTrue(invalidIndexName.getMessage().contains("[ type ] cannot be empty"));
    }

    public void testValidLineString() {
        // Simple LineString with 100 coordinates - should work fine
        JSONArray coordinates = new JSONArray();
        for (int i = 0; i < 100; i++) {
            coordinates.put(new JSONArray().put(i * 0.1).put(45.0));
        }
        
        JSONObject feature = new JSONObject()
            .put("type", "Feature")
            .put("geometry", new JSONObject()
                .put("type", "LineString")
                .put("coordinates", coordinates))
            .put("properties", new JSONObject());
        
        JSONObject request = new JSONObject()
            .put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));
        
        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testLineStringWithTooManyCoordinates() {
        // LineString with 10,001 coordinates - should fail
        JSONArray coordinates = new JSONArray();
        for (int i = 0; i <= 10000; i++) {
            coordinates.put(new JSONArray().put(i * 0.01).put(45.0));
        }
        
        JSONObject feature = new JSONObject()
            .put("type", "Feature")
            .put("geometry", new JSONObject()
                .put("type", "LineString")
                .put("coordinates", coordinates))
            .put("properties", new JSONObject());
        
        JSONObject request = new JSONObject()
            .put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));
        
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
        assertTrue(exception.getMessage().contains("exceeds limit of 10000"));
    }

    public void testValidPolygonWithHoles() {
        // Simple polygon with 10 holes - should work fine
        JSONArray outerRing = new JSONArray()
            .put(new JSONArray().put(-10.0).put(-10.0))
            .put(new JSONArray().put(10.0).put(-10.0))
            .put(new JSONArray().put(10.0).put(10.0))
            .put(new JSONArray().put(-10.0).put(10.0))
            .put(new JSONArray().put(-10.0).put(-10.0));
        
        JSONArray rings = new JSONArray().put(outerRing);
        
        // Add 10 holes
        for (int i = 0; i < 10; i++) {
            JSONArray hole = new JSONArray()
                .put(new JSONArray().put(i * 0.5).put(0.0))
                .put(new JSONArray().put(i * 0.5 + 0.2).put(0.0))
                .put(new JSONArray().put(i * 0.5 + 0.2).put(0.2))
                .put(new JSONArray().put(i * 0.5).put(0.2))
                .put(new JSONArray().put(i * 0.5).put(0.0));
            rings.put(hole);
        }
        
        JSONObject feature = new JSONObject()
            .put("type", "Feature")
            .put("geometry", new JSONObject()
                .put("type", "Polygon")
                .put("coordinates", rings))
            .put("properties", new JSONObject());
        
        JSONObject request = new JSONObject()
            .put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));
        
        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(request.toMap());
        assertNotNull(content);
    }

    public void testPolygonWithTooManyHoles() {
        // Polygon with 1,001 holes - should fail
        JSONArray outerRing = new JSONArray()
            .put(new JSONArray().put(-50.0).put(-50.0))
            .put(new JSONArray().put(50.0).put(-50.0))
            .put(new JSONArray().put(50.0).put(50.0))
            .put(new JSONArray().put(-50.0).put(50.0))
            .put(new JSONArray().put(-50.0).put(-50.0));
        
        JSONArray rings = new JSONArray().put(outerRing);
        
        // Add 1,001 holes
        for (int i = 0; i <= 1000; i++) {
            JSONArray hole = new JSONArray()
                .put(new JSONArray().put(0.0).put(i * 0.01))
                .put(new JSONArray().put(0.1).put(i * 0.01))
                .put(new JSONArray().put(0.1).put(i * 0.01 + 0.1))
                .put(new JSONArray().put(0.0).put(i * 0.01 + 0.1))
                .put(new JSONArray().put(0.0).put(i * 0.01));
            rings.put(hole);
        }
        
        JSONObject feature = new JSONObject()
            .put("type", "Feature")
            .put("geometry", new JSONObject()
                .put("type", "Polygon")
                .put("coordinates", rings))
            .put("properties", new JSONObject());
        
        JSONObject request = new JSONObject()
            .put("index", indexName)
            .put("field", fieldName)
            .put("type", "geo_shape")
            .put("data", new JSONArray().put(feature));
        
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> UploadGeoJSONRequestContent.create(request.toMap())
        );
        assertTrue(exception.getMessage().contains("exceeds limit of 1000"));
    }
}
