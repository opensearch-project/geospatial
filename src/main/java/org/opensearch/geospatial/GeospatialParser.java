/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.HashMap;
import java.util.Map;

public class GeospatialParser {

    public static Map<String, Object> toStringObjectMap(Object input) {
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
}
