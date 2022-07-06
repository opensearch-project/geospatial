/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.geojson;

import static org.opensearch.geospatial.GeospatialParser.toStringObjectMap;
import static org.opensearch.geospatial.geojson.Feature.TYPE;

import java.util.Map;

import lombok.NonNull;

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
     * @throws NullPointerException if input is null
     * @throws IllegalArgumentException if input doesn't have valid arguments
     */
    public static Feature create(@NonNull Map<String, Object> input) {
        Object geoJSONType = input.get(Feature.TYPE_KEY);
        if (geoJSONType == null) {
            throw new IllegalArgumentException(Feature.TYPE_KEY + " cannot be null");
        }
        if (!TYPE.equalsIgnoreCase(geoJSONType.toString())) {
            throw new IllegalArgumentException("Unknown type [ " + geoJSONType + " ], expected type [ " + TYPE + " ]");
        }
        return extractFeature(input).build();
    }

    private static FeatureBuilder extractFeature(Map<String, Object> input) {
        Object geometry = input.get(Feature.GEOMETRY_KEY);
        if (geometry == null) {
            throw new IllegalArgumentException("key: " + Feature.GEOMETRY_KEY + " cannot be null");
        }
        if (!(geometry instanceof Map)) {
            throw new IllegalArgumentException(
                "key: " + Feature.GEOMETRY_KEY + " is not an instance of type Map but of type [ " + geometry.getClass().getName() + " ]"
            );
        }
        Map<String, Object> geometryMap = toStringObjectMap(geometry);
        FeatureBuilder featureBuilder = new FeatureBuilder(geometryMap);
        Object properties = input.get(Feature.PROPERTIES_KEY);
        if (properties == null) {
            return featureBuilder;
        }
        if (!(properties instanceof Map)) {
            throw new IllegalArgumentException(
                "key: " + Feature.PROPERTIES_KEY + " is not an instance of type Map but of type [ " + properties.getClass().getName() + " ]"
            );
        }
        return featureBuilder.properties(toStringObjectMap(properties));
    }
}
