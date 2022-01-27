/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

public class GeospatialParserTests extends OpenSearchTestCase {
    public void testToStringObjectMapInvalidInput() {
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> GeospatialParser.toStringObjectMap("invalid")
        );
        assertTrue(exception.getMessage().contains("is not an instance of Map"));
    }

    private Map<String, Object> getStringObjectMap() {
        Map<String, Object> testData = new HashMap<>();
        testData.put("key1", 1);
        testData.put("key2", "two");
        testData.put("key3", new HashMap<>());
        return testData;
    }

    public void testToStringObjectMap() {
        Map<String, Object> testData = getStringObjectMap();
        Map<String, Object> stringObjectMap = GeospatialParser.toStringObjectMap(testData);
        assertEquals(testData, stringObjectMap);
    }

    public void testExtractValueAsString() {
        final Map<String, Object> testData = getStringObjectMap();
        // assert invalid instance
        {
            IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> GeospatialParser.extractValueAsString(testData, "key1")
            );
            assertTrue(exception.getMessage().contains("is not an instance of String"));
        }
        // assert invalid input
        {
            assertThrows(java.lang.NullPointerException.class, () -> GeospatialParser.extractValueAsString(null, "key1"));
            assertThrows(java.lang.NullPointerException.class, () -> GeospatialParser.extractValueAsString(testData, null));
        }
        // assert key doesn't exist
        {
            assertNull(GeospatialParser.extractValueAsString(testData, "invalid"));
        }

        // assert valid response
        {
            String expectedKey = "key2";
            assertEquals(String.valueOf(testData.get(expectedKey)), GeospatialParser.extractValueAsString(testData, expectedKey));
        }

    }

    public void testConvertToMap() {
        JSONObject input = new JSONObject();
        input.put("index", "test-index");
        Map<String, Object> actualMap = GeospatialParser.convertToMap(new BytesArray(input.toString().getBytes(StandardCharsets.UTF_8)));
        assertEquals(input.toMap(), actualMap);
    }

}
