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

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexingOperationListener;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

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

    private String doGetIndexName(final String datasourceName) {
        return getMetadata().getOrDefault(datasourceName, DatasourceMetadata.EMPTY_METADATA).getIndexName();
    }

    public String getIndexName(final String datasourceName) {
        String indexName = doGetIndexName(datasourceName);
        if (indexName == null) {
            refreshDatasource(datasourceName);
            indexName = doGetIndexName(datasourceName);
        }
        return indexName;
    }

    private boolean doIsExpired(final String datasourceName) {
        final Instant expirationDate = getMetadata().getOrDefault(datasourceName, DatasourceMetadata.EMPTY_METADATA).getExpirationDate();
        final Instant now = Instant.now();
        final boolean isExpired = expirationDate.isBefore(now);
        if (isExpired) {
            log.warn("Datasource {} is expired. Expiration date is {} and now is {}.", datasourceName, expirationDate, now);
        }
        return isExpired;
    }

    public boolean isExpired(final String datasourceName) {
        boolean isExpired = doIsExpired(datasourceName);
        if (isExpired) {
            refreshDatasource(datasourceName);
            isExpired = doIsExpired(datasourceName);
        }
        return isExpired;
    }

    private boolean doHas(final String datasourceName) {
        return getMetadata().containsKey(datasourceName);
    }

    public boolean has(final String datasourceName) {
        boolean isExist = doHas(datasourceName);
        if (isExist == false) {
            refreshDatasource(datasourceName);
            isExist = doHas(datasourceName);
        }
        return isExist;
    }

    private DatasourceState doGetState(final String datasourceName) {
        return getMetadata().getOrDefault(datasourceName, DatasourceMetadata.EMPTY_METADATA).getState();
    }

    public DatasourceState getState(final String datasourceName) {
        DatasourceState state = doGetState(datasourceName);
        if (DatasourceState.AVAILABLE.equals(state) == false) {
            refreshDatasource(datasourceName);
            state = doGetState(datasourceName);
        }
        return state;
    }

    private Map<String, Object> doGetGeoData(final String indexName, final String ip) throws ExecutionException {
        return geoDataCache.putIfAbsent(indexName, ip, addr -> geoIpDataDao.getGeoIpData(indexName, ip));
    }

    public Map<String, Object> getGeoData(final String indexName, final String ip, final String datasourceName) {
        Map<String, Object> geoData;
        try {
            geoData = doGetGeoData(indexName, ip);
        } catch (Exception e) {
            refreshDatasource(datasourceName);
            try {
                geoData = doGetGeoData(indexName, ip);
            } catch (Exception ex) {
                log.error("Fail to get geo data.", e);
                throw new RuntimeException(ex);
            }
        }

        return geoData;
    }

    private Map<String, DatasourceMetadata> getMetadata() {
        // Use a local variable to hold the reference of the metadata in case another thread set the metadata as null,
        // and we unexpectedly return the null. Using this local variable we ensure we return a non-null value.
        Map<String, DatasourceMetadata> currentMetadata = metadata;
        if (currentMetadata != null) {
            return currentMetadata;
        }
        synchronized (this) {
            currentMetadata = metadata;
            if (currentMetadata != null) {
                return currentMetadata;
            }
            currentMetadata = new ConcurrentHashMap<>();
            try {
                for (Datasource datasource : datasourceDao.getAllDatasources()) {
                    currentMetadata.put(datasource.getName(), new DatasourceMetadata(datasource));
                }
            } catch (IndexNotFoundException e) {
                log.debug("Datasource has never been created");
            }
            metadata = currentMetadata;
            return currentMetadata;
        }
    }

    private void put(final Datasource datasource) {
        DatasourceMetadata metadata = new DatasourceMetadata(datasource);
        getMetadata().put(datasource.getName(), metadata);
    }

    private void remove(final String datasourceName) {
        getMetadata().remove(datasourceName);
    }

    private void refreshDatasource(final String datasourceName) {
        try {
            log.info("Refresh datasource.");
            Datasource datasource = datasourceDao.getDatasource(datasourceName);
            if (datasource != null) {
                getMetadata().put(datasourceName, new DatasourceMetadata(datasource));
            } else {
                getMetadata().remove(datasourceName);
            }
        } catch (Exception e) {
            log.error("Fail to refresh the datasource.", e);
            clearMetadata();
        }
    }

    private void clearMetadata() {
        log.info("Resetting all datasource metadata to force a refresh from the primary index shard.");
        metadata = null;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        log.error("Skipped updating datasource metadata for datasource {} due to an indexing exception.", index.id(), ex);
        clearMetadata();
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (Engine.Result.Type.FAILURE.equals(result.getResultType())) {
            log.error(
                "Skipped updating datasource metadata for datasource {} because the indexing result was a failure.",
                index.id(),
                result.getFailure()
            );
            clearMetadata();
            return;
        }

        try {
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, index.source().utf8ToString());
            parser.nextToken();
            Datasource datasource = Datasource.PARSER.parse(parser, null);
            put(datasource);
            log.info("Updated datasource metadata for datasource {} successfully.", index.id());
        } catch (IOException e) {
            log.error("IOException occurred updating datasource metadata for datasource {} ", index.id(), e);
            clearMetadata();
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        log.error("Skipped updating datasource metadata for datasource {} due to an exception.", delete.id(), ex);
        clearMetadata();
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getResultType().equals(Engine.Result.Type.FAILURE)) {
            log.error(
                "Skipped updating datasource metadata for datasource {} because the delete result was a failure.",
                delete.id(),
                result.getFailure()
            );
            clearMetadata();
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
