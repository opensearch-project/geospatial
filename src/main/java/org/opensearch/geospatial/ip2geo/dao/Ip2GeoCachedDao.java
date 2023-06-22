/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.dao;

import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;

/**
 * Data access object for Datasource and GeoIP data with added caching layer
 *
 * Ip2GeoCachedDao has a memory cache to store Datasource and GeoIP data. To fully utilize the cache,
 * do not create multiple Ip2GeoCachedDao. Ip2GeoCachedDao instance is bound to guice so that you can use
 * it through injection.
 *
 * All IP2Geo processors share single Ip2GeoCachedDao instance.
 */
@Log4j2
public class Ip2GeoCachedDao implements IndexingOperationListener {
    private final DatasourceDao datasourceDao;
    private final GeoIpDataDao geoIpDataDao;
    private final GeoDataCache geoDataCache;
    private Map<String, DatasourceMetadata> metadata;

    public Ip2GeoCachedDao(final ClusterService clusterService, final DatasourceDao datasourceDao, final GeoIpDataDao geoIpDataDao) {
        this.datasourceDao = datasourceDao;
        this.geoIpDataDao = geoIpDataDao;
        this.geoDataCache = new GeoDataCache(clusterService.getClusterSettings().get(Ip2GeoSettings.CACHE_SIZE));
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(Ip2GeoSettings.CACHE_SIZE, setting -> this.geoDataCache.updateMaxSize(setting.longValue()));
    }

    public String getIndexName(final String datasourceName) {
        return getMetadata().getOrDefault(datasourceName, DatasourceMetadata.EMPTY_METADATA).getIndexName();
    }

    public boolean isExpired(final String datasourceName) {
        return getMetadata().getOrDefault(datasourceName, DatasourceMetadata.EMPTY_METADATA).getExpirationDate().isBefore(Instant.now());
    }

    public boolean has(final String datasourceName) {
        return getMetadata().containsKey(datasourceName);
    }

    public DatasourceState getState(final String datasourceName) {
        return getMetadata().getOrDefault(datasourceName, DatasourceMetadata.EMPTY_METADATA).getState();
    }

    public Map<String, Object> getGeoData(final String indexName, final String ip) {
        try {
            return geoDataCache.putIfAbsent(indexName, ip, addr -> geoIpDataDao.getGeoIpData(indexName, ip));
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, DatasourceMetadata> getMetadata() {
        if (metadata != null) {
            return metadata;
        }
        synchronized (this) {
            if (metadata != null) {
                return metadata;
            }
            Map<String, DatasourceMetadata> tempData = new ConcurrentHashMap<>();
            try {
                datasourceDao.getAllDatasources()
                    .stream()
                    .forEach(datasource -> tempData.put(datasource.getName(), new DatasourceMetadata(datasource)));
            } catch (IndexNotFoundException e) {
                log.debug("Datasource has never been created");
            }
            metadata = tempData;
            return metadata;
        }
    }

    private void put(final Datasource datasource) {
        DatasourceMetadata metadata = new DatasourceMetadata(datasource);
        getMetadata().put(datasource.getName(), metadata);
    }

    private void remove(final String datasourceName) {
        getMetadata().remove(datasourceName);
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (Engine.Result.Type.FAILURE.equals(result.getResultType())) {
            return;
        }

        try {
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, index.source().utf8ToString());
            parser.nextToken();
            Datasource datasource = Datasource.PARSER.parse(parser, null);
            put(datasource);
        } catch (IOException e) {
            log.error("IOException occurred updating datasource metadata for datasource {} ", index.id(), e);
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getResultType().equals(Engine.Result.Type.FAILURE)) {
            return;
        }
        remove(delete.id());
    }

    @Getter
    private static class DatasourceMetadata {
        private static DatasourceMetadata EMPTY_METADATA = new DatasourceMetadata();
        private String indexName;
        private Instant expirationDate;
        private DatasourceState state;

        private DatasourceMetadata() {
            expirationDate = Instant.MIN;
        }

        public DatasourceMetadata(final Datasource datasource) {
            this.indexName = datasource.currentIndexName();
            this.expirationDate = datasource.expirationDay();
            this.state = datasource.getState();
        }
    }

    /**
     * Cache to hold geo data
     *
     * GeoData in an index in immutable. Therefore, invalidation is not needed.
     */
    @VisibleForTesting
    protected static class GeoDataCache {
        private Cache<CacheKey, Map<String, Object>> cache;

        public GeoDataCache(final long maxSize) {
            if (maxSize < 0) {
                throw new IllegalArgumentException("ip2geo max cache size must be 0 or greater");
            }
            this.cache = CacheBuilder.<CacheKey, Map<String, Object>>builder().setMaximumWeight(maxSize).build();
        }

        public Map<String, Object> putIfAbsent(
            final String indexName,
            final String ip,
            final Function<String, Map<String, Object>> retrieveFunction
        ) throws ExecutionException {
            CacheKey cacheKey = new CacheKey(indexName, ip);
            return cache.computeIfAbsent(cacheKey, key -> retrieveFunction.apply(key.ip));
        }

        public Map<String, Object> get(final String indexName, final String ip) {
            return cache.get(new CacheKey(indexName, ip));
        }

        /**
         * Create a new cache with give size and replace existing cache
         *
         * Try to populate the existing value from previous cache to the new cache in best effort
         *
         * @param maxSize
         */
        public void updateMaxSize(final long maxSize) {
            if (maxSize < 0) {
                throw new IllegalArgumentException("ip2geo max cache size must be 0 or greater");
            }
            Cache<CacheKey, Map<String, Object>> temp = CacheBuilder.<CacheKey, Map<String, Object>>builder()
                .setMaximumWeight(maxSize)
                .build();
            int count = 0;
            Iterator<CacheKey> it = cache.keys().iterator();
            while (it.hasNext() && count < maxSize) {
                CacheKey key = it.next();
                temp.put(key, cache.get(key));
                count++;
            }
            cache = temp;
        }

        @AllArgsConstructor
        @EqualsAndHashCode
        private static class CacheKey {
            private final String indexName;
            private final String ip;
        }
    }
}
