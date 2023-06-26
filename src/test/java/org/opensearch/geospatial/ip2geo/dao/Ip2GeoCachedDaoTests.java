/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.dao;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.ShardId;

public class Ip2GeoCachedDaoTests extends Ip2GeoTestCase {
    private Ip2GeoCachedDao ip2GeoCachedDao;

    @Before
    public void init() {
        ip2GeoCachedDao = new Ip2GeoCachedDao(datasourceDao);
    }

    public void testGetIndexName_whenCalled_thenReturnIndexName() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        String indexName = ip2GeoCachedDao.getIndexName(datasource.getName());

        // Verify
        assertEquals(datasource.currentIndexName(), indexName);
    }

    public void testIsExpired_whenExpired_thenReturnTrue() {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSucceededAt(Instant.MIN);
        datasource.getUpdateStats().setLastSkippedAt(null);
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        boolean isExpired = ip2GeoCachedDao.isExpired(datasource.getName());

        // Verify
        assertTrue(isExpired);
    }

    public void testIsExpired_whenNotExpired_thenReturnFalse() {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSucceededAt(Instant.now());
        datasource.getUpdateStats().setLastSkippedAt(null);
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        boolean isExpired = ip2GeoCachedDao.isExpired(datasource.getName());

        // Verify
        assertFalse(isExpired);
    }

    public void testHas_whenHasDatasource_thenReturnTrue() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        boolean hasDatasource = ip2GeoCachedDao.has(datasource.getName());

        // Verify
        assertTrue(hasDatasource);
    }

    public void testHas_whenNoDatasource_thenReturnFalse() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        // Run
        boolean hasDatasource = ip2GeoCachedDao.has(datasourceName);

        // Verify
        assertFalse(hasDatasource);
    }

    public void testGetState_whenCalled_thenReturnState() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        DatasourceState state = ip2GeoCachedDao.getState(datasource.getName());

        // Verify
        assertEquals(datasource.getState(), state);
    }

    @SneakyThrows
    public void testPostIndex_whenFailed_thenNoUpdate() {
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        Datasource datasource = randomDatasource();

        ShardId shardId = mock(ShardId.class);
        Engine.Index index = mock(Engine.Index.class);
        BytesReference bytesReference = BytesReference.bytes(datasource.toXContent(XContentFactory.jsonBuilder(), null));
        when(index.source()).thenReturn(bytesReference);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.FAILURE);

        // Run
        ip2GeoCachedDao.postIndex(shardId, index, result);

        // Verify
        assertFalse(ip2GeoCachedDao.has(datasource.getName()));
        assertTrue(ip2GeoCachedDao.isExpired(datasource.getName()));
        assertNull(ip2GeoCachedDao.getIndexName(datasource.getName()));
        assertNull(ip2GeoCachedDao.getState(datasource.getName()));
    }

    @SneakyThrows
    public void testPostIndex_whenSucceed_thenUpdate() {
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        Datasource datasource = randomDatasource();

        ShardId shardId = mock(ShardId.class);
        Engine.Index index = mock(Engine.Index.class);
        BytesReference bytesReference = BytesReference.bytes(datasource.toXContent(XContentFactory.jsonBuilder(), null));
        when(index.source()).thenReturn(bytesReference);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        // Run
        ip2GeoCachedDao.postIndex(shardId, index, result);

        // Verify
        assertTrue(ip2GeoCachedDao.has(datasource.getName()));
        assertFalse(ip2GeoCachedDao.isExpired(datasource.getName()));
        assertEquals(datasource.currentIndexName(), ip2GeoCachedDao.getIndexName(datasource.getName()));
        assertEquals(datasource.getState(), ip2GeoCachedDao.getState(datasource.getName()));
    }

    public void testPostDelete_whenFailed_thenNoUpdate() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        ShardId shardId = mock(ShardId.class);
        Engine.Delete index = mock(Engine.Delete.class);
        Engine.DeleteResult result = mock(Engine.DeleteResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.FAILURE);

        // Run
        ip2GeoCachedDao.postDelete(shardId, index, result);

        // Verify
        assertTrue(ip2GeoCachedDao.has(datasource.getName()));
    }

    public void testPostDelete_whenSucceed_thenUpdate() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        ShardId shardId = mock(ShardId.class);
        Engine.Delete index = mock(Engine.Delete.class);
        when(index.id()).thenReturn(datasource.getName());
        Engine.DeleteResult result = mock(Engine.DeleteResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.SUCCESS);

        // Run
        ip2GeoCachedDao.postDelete(shardId, index, result);

        // Verify
        assertFalse(ip2GeoCachedDao.has(datasource.getName()));
    }

    @SneakyThrows
    public void testUpdateMaxSize_whenBiggerSize_thenContainsAllData() {
        int cacheSize = 10;
        String datasource = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoCachedDao.GeoDataCache geoDataCache = new Ip2GeoCachedDao.GeoDataCache(cacheSize);
        List<String> ips = new ArrayList<>(cacheSize);
        for (int i = 0; i < cacheSize; i++) {
            String ip = NetworkAddress.format(randomIp(false));
            ips.add(ip);
            geoDataCache.putIfAbsent(datasource, ip, addr -> Collections.emptyMap());
        }

        // Verify all data exist in the cache
        assertTrue(ips.stream().allMatch(ip -> geoDataCache.get(datasource, ip) != null));

        // Update cache size
        int newCacheSize = 15;
        geoDataCache.updateMaxSize(newCacheSize);

        // Verify all data exist in the cache
        assertTrue(ips.stream().allMatch(ip -> geoDataCache.get(datasource, ip) != null));

        // Add (newCacheSize - cacheSize + 1) data and the first data should not be available in the cache
        for (int i = 0; i < newCacheSize - cacheSize + 1; i++) {
            geoDataCache.putIfAbsent(datasource, NetworkAddress.format(randomIp(false)), addr -> Collections.emptyMap());
        }
        assertNull(geoDataCache.get(datasource, ips.get(0)));
    }

    @SneakyThrows
    public void testUpdateMaxSize_whenSmallerSize_thenContainsPartialData() {
        int cacheSize = 10;
        String datasource = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoCachedDao.GeoDataCache geoDataCache = new Ip2GeoCachedDao.GeoDataCache(cacheSize);
        List<String> ips = new ArrayList<>(cacheSize);
        for (int i = 0; i < cacheSize; i++) {
            String ip = NetworkAddress.format(randomIp(false));
            ips.add(ip);
            geoDataCache.putIfAbsent(datasource, ip, addr -> Collections.emptyMap());
        }

        // Verify all data exist in the cache
        assertTrue(ips.stream().allMatch(ip -> geoDataCache.get(datasource, ip) != null));

        // Update cache size
        int newCacheSize = 5;
        geoDataCache.updateMaxSize(newCacheSize);

        // Verify the last (cacheSize - newCacheSize) data is available in the cache
        List<String> deleted = ips.subList(0, ips.size() - newCacheSize);
        List<String> retained = ips.subList(ips.size() - newCacheSize, ips.size());
        assertTrue(deleted.stream().allMatch(ip -> geoDataCache.get(datasource, ip) == null));
        assertTrue(retained.stream().allMatch(ip -> geoDataCache.get(datasource, ip) != null));
    }
}
