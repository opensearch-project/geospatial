/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.Map;
import java.util.stream.IntStream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.common.Randomness;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.List;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.geo.GeometryTestUtils;
import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.geospatial.geojson.FeatureCollection;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

/**
 * GeospatialObjectBuilder contains utility methods to generate geospatial objects
 * for tests
 */
public class GeospatialObjectBuilder {

    public static final String GEOMETRY_TYPE_KEY = "type";
    public static final String GEOMETRY_COORDINATES_KEY = "coordinates";
    public static final int MIN_LINE_STRING_COORDINATES_SIZE = 2;
    public static final int MAX_POINTS = 10;
    public static final int POINTS_SIZE = 2;

    public static JSONObject buildGeometry(String type, Object value) {
        JSONObject geometry = new JSONObject();
        geometry.put(GEOMETRY_TYPE_KEY, type);
        geometry.put(GEOMETRY_COORDINATES_KEY, value);
        return geometry;
    }

    public static JSONObject randomGeometryPoint() {
        return buildGeometry(GeoShapeType.POINT.shapeName(), getRandomPoint());
    }

    public static JSONObject randomGeometryLineString() {
        int randomTotalPoints = randomBoundedInt(MIN_LINE_STRING_COORDINATES_SIZE, MAX_POINTS);
        double[][] lineString = new double[randomTotalPoints][POINTS_SIZE];
        IntStream.range(0, lineString.length).forEach(index -> lineString[index] = getRandomPoint());
        return buildGeometry(GeoShapeType.LINESTRING.shapeName(), lineString);
    }

    private static double[] getRandomPoint() {
        return new double[] { GeometryTestUtils.randomLat(), GeometryTestUtils.randomLon() };
    }

    public static JSONObject buildGeoJSONFeature(JSONObject geometry, JSONObject properties) {
        JSONObject feature = new JSONObject();
        feature.put(Feature.TYPE_KEY, Feature.TYPE);
        feature.put(Feature.GEOMETRY_KEY, geometry);
        feature.put(Feature.PROPERTIES_KEY, properties);
        return feature;
    }

    public static JSONObject buildProperties(Map<String, Object> properties) {
        JSONObject propertiesObject = new JSONObject();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            propertiesObject.put(entry.getKey(), entry.getValue());
        }
        return propertiesObject;
    }

    public static JSONObject buildGeoJSONFeatureCollection(JSONArray features) {
        JSONObject collection = new JSONObject();
        collection.put(FeatureCollection.TYPE_KEY, FeatureCollection.TYPE);
        collection.put(FeatureCollection.FEATURES_KEY, features.toList().toArray());
        return collection;
    }

    public static int randomBoundedInt(int min, int max) {
        return Randomness.get().ints(min, max).findFirst().getAsInt();
    }

    public static JSONObject randomGeoJSONFeature(final JSONObject properties, String featureId) {
        JSONObject geoJSONFeature = buildGeoJSONFeature(randomGeoJSONGeometry(), properties);
        if (!Strings.hasText(featureId)) {
            return geoJSONFeature;
        }
        geoJSONFeature.put(featureId, UUIDs.randomBase64UUID());
        return geoJSONFeature;
    }

    public static JSONObject randomGeoJSONFeature(final JSONObject properties) {
        return randomGeoJSONFeature(properties, null);
    }

    public static JSONObject randomGeoJSONGeometry() {
        return RandomPicks.randomFrom(Randomness.get(), List.of(randomGeometryLineString(), randomGeometryPoint()));
    }
}
