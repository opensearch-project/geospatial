/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.geojson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.opensearch.geospatial.GeospatialParser;

/**
 * FeatureCollection represents GEOJSON of type FeatureCollection. A FeatureCollection object has a member
 * with the name "features".  The value of "features" is a List.
 * and it is possible for this list to be empty, if {@link FeatureCollection} has no Features.
 */
public final class FeatureCollection {
    public static final String TYPE = "FeatureCollection";
    public static final String FEATURES_KEY = "features";
    public static final String TYPE_KEY = "type";
    private final List<Map<String, Object>> features;

    private FeatureCollection() {
        this.features = new ArrayList<>();
    }

    /**
     * Gets the list of Features in Map format
     *
     * @return List of Feature as Map from the {@link FeatureCollection}
     */
    public List<Map<String, Object>> getFeatures() {
        return features;
    }

    /**
     * Add Features to this collection
     * @param featureMap feature in Map format
     * @throws NullPointerException if feature is null
     */
    public void addFeature(Map<String, Object> featureMap) {
        Objects.requireNonNull(featureMap, "cannot add null to features");
        this.features.add(featureMap);
    }

    /**
     * The static method to create an instance of {@link FeatureCollection} from input.
     *
     * @param input the object from where {@link FeatureCollection} will be extracted
     * @return FeatureCollection Instance from given input
     * @throws NullPointerException if input is null
     * @throws IllegalArgumentException if input doesn't have valid arguments
     */
    public static FeatureCollection create(final Map<String, Object> input) {
        Objects.requireNonNull(input, "input cannot be null");
        Object geoJSONType = input.get(TYPE_KEY);
        if (geoJSONType == null) {
            throw new IllegalArgumentException(TYPE_KEY + " cannot be null");
        }
        if (!TYPE.equalsIgnoreCase(geoJSONType.toString())) {
            throw new IllegalArgumentException("Unknown type [ " + geoJSONType + " ], expected type [ " + TYPE + " ]");
        }
        return extract(input);
    }

    private static FeatureCollection extract(Map<String, Object> input) {
        FeatureCollection collection = new FeatureCollection();
        Object featureObject = input.get(FEATURES_KEY);
        if (featureObject == null) { // empty features are valid based on definition
            return collection;
        }
        if (!(featureObject instanceof List)) {
            throw new IllegalArgumentException(
                FEATURES_KEY + " is not an instance of type List, but of type [ " + featureObject.getClass().getName() + " ]"
            );
        }
        List<Object> featureArray = (List) featureObject;
        featureArray.stream().map(GeospatialParser::toStringObjectMap).forEach(collection::addFeature);
        return collection;
    }

}
