/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.geojson;

import static org.opensearch.geospatial.GeospatialObjectBuilder.buildGeoJSONFeatureCollection;
import static org.opensearch.geospatial.GeospatialObjectBuilder.randomGeoJSONFeature;
import static org.opensearch.geospatial.geojson.FeatureCollection.FEATURES_KEY;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.test.OpenSearchTestCase;

public class FeatureCollectionTests extends OpenSearchTestCase {

    public void testCreateFeatureCollection() {
        JSONArray features = new JSONArray();
        features.put(randomGeoJSONFeature(new JSONObject()));
        features.put(randomGeoJSONFeature(new JSONObject()));
        features.put(randomGeoJSONFeature(new JSONObject()));

        Map<String, Object> featureCollectionAsMap = buildGeoJSONFeatureCollection(features).toMap();
        FeatureCollection collection = FeatureCollection.create(featureCollectionAsMap);
        assertNotNull(collection);
        assertEquals(features.toList().size(), collection.getFeatures().size());
    }

    public void testCreateFeatureCollectionInvalidType() {
        Map<String, Object> featureCollectionAsMap = new HashMap<>();
        featureCollectionAsMap.put(FeatureCollection.TYPE_KEY, Feature.TYPE);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> FeatureCollection.create(featureCollectionAsMap));
        assertTrue(ex.getMessage().contains("expected type [ FeatureCollection ]"));
    }

    public void testCreateFeatureCollectionInvalidInput() {
        NullPointerException ex = assertThrows(NullPointerException.class, () -> FeatureCollection.create(null));
        assertTrue(ex.getMessage().equals("input cannot be null"));
    }

    public void testCreateFeatureCollectionTypeMissing() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> FeatureCollection.create(new HashMap<>()));
        assertTrue(ex.getMessage().contains("type cannot be null"));
    }

    public void testCreateFeatureCollectionInvalidFeature() {
        Map<String, Object> featureCollectionAsMap = new HashMap<>();
        featureCollectionAsMap.put(FeatureCollection.TYPE_KEY, FeatureCollection.TYPE);
        featureCollectionAsMap.put(FEATURES_KEY, "invalid");
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> FeatureCollection.create(featureCollectionAsMap));
        assertTrue(ex.getMessage().contains(FEATURES_KEY + " is not an instance of type List"));
    }
}
