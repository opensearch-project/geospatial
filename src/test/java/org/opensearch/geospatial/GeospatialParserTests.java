/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.test.OpenSearchTestCase;

public class GeospatialParserTests extends OpenSearchTestCase {
    public void testToStringObjectMapInvalidInput() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> GeospatialParser.toStringObjectMap("invalid")
        );
        assertTrue(exception.getMessage().contains("is not an instance of Map"));
    }

    private Map<String, Object> getStringObjectMap() {
        Map<String, Object> testData = new HashMap<>();
        testData.put("key1", 1);
        testData.put("key2", "two");
        testData.put("key3", new HashMap<>());
        return testData;
    }

    public void testToStringObjectMap() {
        Map<String, Object> testData = getStringObjectMap();
        Map<String, Object> stringObjectMap = GeospatialParser.toStringObjectMap(testData);
        assertEquals(testData, stringObjectMap);
    }

    public void testExtractValueAsString() {
        final Map<String, Object> testData = getStringObjectMap();
        // assert invalid instance
        {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> GeospatialParser.extractValueAsString(testData, "key1")
            );
            assertTrue(exception.getMessage().contains("is not an instance of String"));
        }
        // assert invalid input
        {
            assertThrows(java.lang.NullPointerException.class, () -> GeospatialParser.extractValueAsString(null, "key1"));
            assertThrows(java.lang.NullPointerException.class, () -> GeospatialParser.extractValueAsString(testData, null));
        }
        // assert key doesn't exist
        {
            assertNull(GeospatialParser.extractValueAsString(testData, "invalid"));
        }

        // assert valid response
        {
            String expectedKey = "key2";
            assertEquals(String.valueOf(testData.get(expectedKey)), GeospatialParser.extractValueAsString(testData, expectedKey));
        }

    }

    public void testConvertToMap() {
        JSONObject input = new JSONObject();
        input.put("index", "test-index");
        Map<String, Object> actualMap = GeospatialParser.convertToMap(new BytesArray(input.toString().getBytes(StandardCharsets.UTF_8)));
        assertEquals(input.toMap(), actualMap);
    }

    public void testGetFeaturesWithGeoJSONFeature() {
        Map<String, Object> geoJSON = GeospatialObjectBuilder.randomGeoJSONFeature(new JSONObject()).toMap();
        Optional<List<Map<String, Object>>> features = GeospatialParser.getFeatures(geoJSON);
        assertTrue(features.isPresent());
        assertTrue(features.get().size() == 1);
        assertEquals(features.get().get(0), geoJSON);
    }

    public void testGetFeaturesWithGeoJSONFeatureCollection() {
        JSONArray features = new JSONArray();
        features.put(GeospatialObjectBuilder.randomGeoJSONFeature(new JSONObject()));
        features.put(GeospatialObjectBuilder.randomGeoJSONFeature(new JSONObject()));
        features.put(GeospatialObjectBuilder.randomGeoJSONFeature(new JSONObject()));

        JSONObject collection = GeospatialObjectBuilder.buildGeoJSONFeatureCollection(features);
        Optional<List<Map<String, Object>>> featureList = GeospatialParser.getFeatures(collection.toMap());
        assertTrue(featureList.isPresent());
        assertTrue(featureList.get().size() == features.length());
    }

    public void testGetFeaturesWithUnSupportedType() {
        Map<String, Object> geoJSON = new HashMap<>();
        geoJSON.put(Feature.TYPE_KEY, "invalid-type");
        Optional<List<Map<String, Object>>> features = GeospatialParser.getFeatures(geoJSON);
        assertFalse(features.isPresent());
    }

}
