/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.test.OpenSearchTestCase;

public class GeospatialParserTests extends OpenSearchTestCase {
    public void testToStringObjectMapInvalidInput() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> GeospatialParser.toStringObjectMap("invalid")
        );
        assertTrue(exception.getMessage().contains("is not an instance of Map"));
    }

    public void testToStringObjectMap() {
        Map<String, Object> testData = new HashMap<>();
        testData.put("key1", 1);
        testData.put("key2", "two");
        testData.put("key3", new HashMap<>());

        Map<String, Object> stringObjectMap = GeospatialParser.toStringObjectMap(testData);
        assertEquals(testData, stringObjectMap);
    }
}
