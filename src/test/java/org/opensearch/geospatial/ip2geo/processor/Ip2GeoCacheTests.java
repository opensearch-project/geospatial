/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.processor;

import java.util.HashMap;
import java.util.Map;

import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

public class Ip2GeoCacheTests extends OpenSearchTestCase {
    public void testCachesAndEvictsResults() {
        Ip2GeoCache cache = new Ip2GeoCache(1);
        String datasource = "datasource";
        Map<String, Object> response1 = new HashMap<>();
        Map<String, Object> response2 = new HashMap<>();
        assertNotSame(response1, response2);

        // add a key
        Map<String, Object> cachedResponse = cache.putIfAbsent("127.0.0.1", datasource, key -> response1);
        assertSame(cachedResponse, response1);
        assertSame(cachedResponse, cache.putIfAbsent("127.0.0.1", datasource, key -> response2));
        assertSame(cachedResponse, cache.get("127.0.0.1", datasource));

        // evict old key by adding another value
        cachedResponse = cache.putIfAbsent("127.0.0.2", datasource, key -> response2);
        assertSame(cachedResponse, response2);
        assertSame(cachedResponse, cache.putIfAbsent("127.0.0.2", datasource, ip -> response2));
        assertSame(cachedResponse, cache.get("127.0.0.2", datasource));

        assertNotSame(response1, cache.get("127.0.0.1", datasource));
    }

    public void testThrowsFunctionsException() {
        Ip2GeoCache cache = new Ip2GeoCache(1);
        expectThrows(
            OpenSearchException.class,
            () -> cache.putIfAbsent("127.0.0.1", "datasource", ip -> { throw new OpenSearchException("bad"); })
        );
    }

    public void testNoExceptionForNullValue() {
        Ip2GeoCache cache = new Ip2GeoCache(1);
        Map<String, Object> response = cache.putIfAbsent("127.0.0.1", "datasource", ip -> null);
        assertTrue(response.isEmpty());
    }

    public void testInvalidInit() {
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> new Ip2GeoCache(-1));
        assertEquals("ip2geo max cache size must be 0 or greater", ex.getMessage());
    }
}
