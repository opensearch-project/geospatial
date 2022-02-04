/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import static org.opensearch.geospatial.geojson.Feature.TYPE_KEY;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.geojson.Feature;
import org.opensearch.geospatial.geojson.FeatureCollection;

/**
 * GeospatialParser provides helper methods to parse/extract/transform input to
 * desired formats.
 */
public final class GeospatialParser {

    /**
     * Convert object into Map
     * @param input Object that is also an instance of Map
     * @return input object in Map type
     */
    public static Map<String, Object> toStringObjectMap(final Object input) {
        if (!(input instanceof Map)) {
            throw new IllegalArgumentException(input + " is not an instance of Map, but of type [ " + input.getClass().getName() + " ]");
        }
        Map<Object, Object> inputMap = (Map<Object, Object>) input;
        Map<String, Object> stringObjectMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : inputMap.entrySet()) {
            stringObjectMap.put(entry.getKey().toString(), entry.getValue());
        }
        return stringObjectMap;
    }

    /**
     * User inputs are usually deserialized into Map. extractValueAsString will help caller to
     * extract value from the Map and cast it to string with validation.
     * @param input User input of type Map, cannot be null
     * @param key property we would like to extract value of, cannot be null
     * @return null if key doesn't exist, value as String if it exists, throw exception otherwise
     */
    public static String extractValueAsString(final Map<String, Object> input, final String key) {
        Objects.requireNonNull(key, "parameter 'key' cannot be null");
        Objects.requireNonNull(input, "parameter 'input' cannot be null");
        Object value = input.get(key);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw new IllegalArgumentException(value + " is not an instance of String, but of type [ " + value.getClass().getName() + " ]");
        }
        return value.toString();
    }

    /**
     * Converts JSON Content from BytesReference to Map
     * @param content JSON Content abstracted as BytesRefernce mostly by REST Interface
     * @return JSON Content as Map
     */
    public static Map<String, Object> convertToMap(BytesReference content) {
        Objects.requireNonNull(content);
        return XContentHelper.convertToMap(content, false, XContentType.JSON).v2();
    }

    /**
     * getFeatures will return features from given map input. This function abstracts the logic to parse given input and returns
     * list of Features if exists in Map format.
     * @param geoJSON given input which may contain GeoJSON Object
     * @return Optional List of Feature in Map, if Feature exist
     */
    public static Optional<List<Map<String, Object>>> getFeatures(final Map<String, Object> geoJSON) {
        final String type = extractValueAsString(geoJSON, TYPE_KEY);
        Objects.requireNonNull(type, TYPE_KEY + " cannot be null");
        if (Feature.TYPE.equalsIgnoreCase(type)) {
            return Optional.ofNullable(Collections.unmodifiableList(Arrays.asList(geoJSON)));
        }
        if (FeatureCollection.TYPE.equalsIgnoreCase(type)) {
            return Optional.ofNullable(Collections.unmodifiableList(FeatureCollection.create(geoJSON).getFeatures()));
        }
        return Optional.empty();
    }
}
