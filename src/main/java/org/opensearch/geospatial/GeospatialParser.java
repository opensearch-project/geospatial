/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

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
     * @param input User input of type Map
     * @param key property we would like to extract value of
     * @return null if key doesn't exist, value as String if it exists, throw exception otherwise
     */
    public static String extractValueAsString(final Map<String, Object> input, final String key) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(input);
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
}
