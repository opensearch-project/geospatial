/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.geojson;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.geospatial.geojson.Feature.FeatureBuilder;

/**
 * FeatureFactory helps to create {@link Feature} instance based on user input
 */
public class FeatureFactory {

    /**
     * The factory method to create an instance of Feature from input.
     *
     * @param input the object from where {@link Feature} will be extracted
     * @return Feature Instance from input
     */
    public static Feature create(Map<String, Object> input) {
        Object geoJSONType = input.get(Feature.TYPE_KEY);
        if (geoJSONType == null) {
            throw new IllegalArgumentException(Feature.TYPE_KEY + " cannot be null");
        }
        if (!Feature.TYPE.equalsIgnoreCase(geoJSONType.toString())) {
            throw new IllegalArgumentException(geoJSONType + " is not supported. Only type " + Feature.TYPE + " is supported");
        }
        return extractFeature(input).build();
    }

    private static Map<String, Object> toStringObjectMap(Object input) {
        if (!(input instanceof Map)) {
            throw new IllegalArgumentException(input + " is not an instance of Map");
        }
        Map<Object, Object> inputMap = (Map<Object, Object>) input;
        Map<String, Object> stringObjectMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : inputMap.entrySet()) {
            stringObjectMap.put(entry.getKey().toString(), entry.getValue());
        }
        return stringObjectMap;
    }

    private static FeatureBuilder extractFeature(Map<String, Object> input) {
        Object geometry = input.get(Feature.GEOMETRY_KEY);
        if (geometry == null) {
            throw new IllegalArgumentException("key: " + Feature.GEOMETRY_KEY + " cannot be null");
        }
        if (!(geometry instanceof Map)) {
            throw new IllegalArgumentException("key: " + Feature.GEOMETRY_KEY + "is not an instance of type Map");
        }
        Map<String, Object> geometryMap = toStringObjectMap(geometry);
        FeatureBuilder featureBuilder = new FeatureBuilder(geometryMap);
        Object properties = input.get(Feature.PROPERTIES_KEY);
        if (properties == null) {
            return featureBuilder;
        }
        if (!(properties instanceof Map)) {
            throw new IllegalArgumentException("key: " + Feature.PROPERTIES_KEY + "is not an instance of type Map");
        }
        return featureBuilder.properties(toStringObjectMap(properties));
    }
}
