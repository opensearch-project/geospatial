/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.Map;
import java.util.Random;

import org.json.JSONObject;
import org.opensearch.common.Randomness;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.geospatial.geojson.Feature;

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

    public static JSONObject getRandomGeometryPoint() {
        Random random = Randomness.get();
        double[] point = new double[] { random.nextDouble(), random.nextDouble() };
        return buildGeometry(GeoShapeType.POINT.shapeName(), point);
    }

    public static JSONObject getRandomGeometryLineString() {
        int randomTotalPoints = randomPositiveInt(MAX_POINTS);
        int randomPointsDimension = randomPositiveInt(MAX_DIMENSION);
        double[][] lineString = new double[randomTotalPoints][randomPointsDimension];
        for (int i = 0; i < lineString.length; i++) {
            lineString[i] = getRandomPoint();
        }
        return buildGeometry(GeoShapeType.LINESTRING.shapeName(), lineString);
    }

    private static double[] getRandomPoint() {
        Random random = Randomness.get();
        return new double[] { random.nextDouble(), random.nextDouble() };
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

    public static int randomPositiveInt(int bound) {
        return Randomness.get().ints(MIN_POSITIVE_INTEGER_VALUE, bound).findFirst().getAsInt();
    }
}
