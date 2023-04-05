/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.processor;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.unit.TimeValue;

/**
 * The in-memory cache for the ip2geo data. There should only be 1 instance of this class.
 */
public class Ip2GeoCache {
    private static final TimeValue CACHING_PERIOD = TimeValue.timeValueMinutes(1);
    private final Cache<CacheKey, Map<String, Object>> cache;

    /**
     * Default constructor
     *
     * @param maxSize size of a cache
     */
    public Ip2GeoCache(final long maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("ip2geo max cache size must be 0 or greater");
        }
        this.cache = CacheBuilder.<CacheKey, Map<String, Object>>builder()
            .setMaximumWeight(maxSize)
            .setExpireAfterWrite(CACHING_PERIOD)
            .build();
    }

    /**
     * Put data in a cache if it is absent and return the data
     *
     * @param ip the first part of a key
     * @param datasourceName the second part of a key
     * @param retrieveFunction function to retrieve a data to be stored in a cache
     * @return data in a cache
     */
    public Map<String, Object> putIfAbsent(
        final String ip,
        final String datasourceName,
        final Function<String, Map<String, Object>> retrieveFunction
    ) {
        CacheKey cacheKey = new CacheKey(ip, datasourceName);
        Map<String, Object> response = cache.get(cacheKey);
        if (response == null) {
            response = retrieveFunction.apply(ip);
            response = response == null ? Collections.emptyMap() : response;
            cache.put(cacheKey, response);
        }
        return response;
    }

    /**
     * Put data in a cache
     *
     * @param ip the first part of a key
     * @param datasourceName the second part of a key
     * @param data the data
     */
    public void put(final String ip, final String datasourceName, final Map<String, Object> data) {
        CacheKey cacheKey = new CacheKey(ip, datasourceName);
        cache.put(cacheKey, data);
    }

    protected Map<String, Object> get(final String ip, final String datasourceName) {
        CacheKey cacheKey = new CacheKey(ip, datasourceName);
        return cache.get(cacheKey);
    }

    /**
     * The key to use for the cache. Since this cache can span multiple ip2geo processors that all use different datasource, the datasource
     * name is needed to be included in the cache key. For example, if we only used the IP address as the key the same IP may be in multiple
     * datasource with different values. The datasource name scopes the IP to the correct datasource
     */
    private static class CacheKey {

        private final String ip;
        private final String datasourceName;

        private CacheKey(final String ip, final String datasourceName) {
            this.ip = ip;
            this.datasourceName = datasourceName;
        }

        // generated
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(ip, cacheKey.ip) && Objects.equals(datasourceName, cacheKey.datasourceName);
        }

        // generated
        @Override
        public int hashCode() {
            return Objects.hash(ip, datasourceName);
        }
    }
}
