/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.HashMap;
import java.util.Map;

/**
 * GeospatialParser provides helper methods to parse/extract/transform input to
 * desired formats.
 */
public class GeospatialParser {

    /**
     * Convert object into Map
     * @param input Object that is also an instance of Map
     * @return input object in Map type
     */
    public static Map<String, Object> toStringObjectMap(Object input) {
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
}
