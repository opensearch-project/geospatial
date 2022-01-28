/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.Map;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.common.Randomness;
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
    public static final int MIN_POSITIVE_INTEGER_VALUE = 1;
    public static final int MAX_POINTS = 10;
    public static final int MAX_DIMENSION = 4;

    public static JSONObject buildGeometry(String type, Object value) {
        JSONObject geometry = new JSONObject();
        geometry.put(GEOMETRY_TYPE_KEY, type);
        geometry.put(GEOMETRY_COORDINATES_KEY, value);
        return geometry;
    }

    public static JSONObject randomGeometryPoint() {
        Random random = Randomness.get();
        double[] point = new double[] { random.nextDouble(), random.nextDouble() };
        return buildGeometry(GeoShapeType.POINT.shapeName(), point);
    }

    public static JSONObject randomGeometryLineString() {
        int randomTotalPoints = randomPositiveInt(MAX_POINTS);
        int randomPointsDimension = randomPositiveInt(MAX_DIMENSION);
        double[][] lineString = new double[randomTotalPoints][randomPointsDimension];
        for (int i = 0; i < lineString.length; i++) {
            lineString[i] = getRandomPoint();
        }
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

    public static int randomPositiveInt(int bound) {
        return Randomness.get().ints(MIN_POSITIVE_INTEGER_VALUE, bound).findFirst().getAsInt();
    }

    public static JSONObject randomGeoJSONFeature(final JSONObject properties) {
        return buildGeoJSONFeature(randomGeoJSONGeometry(), properties);
    }

    public static JSONObject randomGeoJSONGeometry() {
        return RandomPicks.randomFrom(Randomness.get(), List.of(randomGeometryLineString(), randomGeometryPoint()));
    }
}
