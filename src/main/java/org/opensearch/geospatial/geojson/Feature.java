/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.geojson;

import java.util.HashMap;
import java.util.Map;

/**
 * Feature object represents GEOJSON of type Feature.
 */
public class Feature {
    public static final String TYPE = "Feature";
    public static final String GEOMETRY_KEY = "geometry";
    public static final String PROPERTIES_KEY = "properties";
    public static final String TYPE_KEY = "type";
    private final Map<String, Object> geometry;
    private final Map<String, Object> properties;

    private Feature(Map<String, Object> geometry) {
        this.geometry = new HashMap<>();
        this.geometry.putAll(geometry);
        this.properties = new HashMap<>();
    }

    /**
     * Gets the geometry value of this Feature
     *
     * @return Feature's Geometry as Map
     */
    public Map<String, Object> getGeometry() {
        return geometry;
    }

    /**
     * Gets the properties of this Feature
     *
     * @return Feature's properties as Map
     */
    public Map<String, Object> getProperties() {
        return properties;
    }

    /**
     * FeatureBuilder represents the Builder Object to build Feature instance
     */
    static class FeatureBuilder {

        private final Feature feature;

        public FeatureBuilder(Map<String, Object> geometry) {
            this.feature = new Feature(geometry);
        }

        /**
         * Returns Feature instance based on FeatureBuilder values.
         *
         * @return Feature, which is a GeoJSON Object of type Feature
         */
        public Feature build() {
            return feature;
        }

        /**
         * Adds given properties to existing properties
         *
         * @param properties to be included in Feature
         * @return FeatureBuilder instance
         */
        public FeatureBuilder properties(Map<String, Object> properties) {
            this.feature.properties.putAll(properties);
            return this;
        }
    }
}
