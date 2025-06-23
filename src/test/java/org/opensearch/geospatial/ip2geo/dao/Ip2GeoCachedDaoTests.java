/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.dao;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.common.network.NetworkAddress;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.engine.Engine;

import lombok.SneakyThrows;

public class Ip2GeoCachedDaoTests extends Ip2GeoTestCase {
    private Ip2GeoCachedDao ip2GeoCachedDao;

    @Before
    public void init() {
        ip2GeoCachedDao = new Ip2GeoCachedDao(clusterService, datasourceDao, geoIpDataDao);
    }

    public void testGetIndexName_whenCalled_thenReturnIndexName() throws IOException {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        String indexName = ip2GeoCachedDao.getIndexName(datasource.getName());

        // Verify
        assertEquals(datasource.currentIndexName(), indexName);
        // Verify datasource refresh is not triggered
        verify(datasourceDao, times(0)).getDatasource(any());
    }

    public void testGetIndexName_whenIndexNotFound_thenReturnNull() throws IOException {
        when(datasourceDao.getAllDatasources()).thenThrow(new IndexNotFoundException("not found"));

        // Run
        String indexName = ip2GeoCachedDao.getIndexName(GeospatialTestHelper.randomLowerCaseString());

        // Verify
        assertNull(indexName);
        // Verify datasource refresh is triggered
        verify(datasourceDao, times(1)).getDatasource(any());
    }

    public void testGetIndexName_whenIndexNotFoundAndFoundAfterRefresh_thenReturnNull() throws IOException {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenThrow(new IndexNotFoundException("not found"));
        when(datasourceDao.getDatasource(any())).thenReturn(datasource);

        // Run
        String indexName = ip2GeoCachedDao.getIndexName(GeospatialTestHelper.randomLowerCaseString());

        // Verify
        assertEquals(datasource.currentIndexName(), indexName);
        // Verify datasource refresh is triggered
        verify(datasourceDao, times(1)).getDatasource(any());
    }

    public void testIsExpired_whenExpired_thenReturnTrue() throws IOException {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSucceededAt(Instant.MIN);
        datasource.getUpdateStats().setLastSkippedAt(null);
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        boolean isExpired = ip2GeoCachedDao.isExpired(datasource.getName());

        // Verify
        assertTrue(isExpired);
        // Verify datasource refresh is triggered
        verify(datasourceDao, times(1)).getDatasource(any());
    }

    public void testIsExpired_whenNotExpired_thenReturnFalse() throws IOException {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSucceededAt(Instant.now());
        datasource.getUpdateStats().setLastSkippedAt(null);
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        boolean isExpired = ip2GeoCachedDao.isExpired(datasource.getName());

        // Verify
        assertFalse(isExpired);
        // Verify datasource refresh is not triggered
        verify(datasourceDao, times(0)).getDatasource(any());
    }

    public void testHas_whenHasDatasource_thenReturnTrue() throws IOException {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        boolean hasDatasource = ip2GeoCachedDao.has(datasource.getName());

        // Verify
        assertTrue(hasDatasource);
        // Verify datasource refresh is not triggered
        verify(datasourceDao, times(0)).getDatasource(any());
    }

    public void testHas_whenNoDatasource_thenReturnFalse() throws IOException {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        // Run
        boolean hasDatasource = ip2GeoCachedDao.has(datasourceName);

        // Verify
        assertFalse(hasDatasource);
        // Verify datasource refresh is triggered
        verify(datasourceDao, times(1)).getDatasource(any());
    }

    public void testGetState_whenCalled_thenReturnState() throws IOException {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));

        // Run
        DatasourceState state = ip2GeoCachedDao.getState(datasource.getName());

        // Verify
        assertEquals(datasource.getState(), state);
        // Verify datasource refresh is not triggered
        verify(datasourceDao, times(0)).getDatasource(any());
    }

    public void testGetGeoData_whenCalled_thenReturnGeoData() throws IOException {
        Datasource datasource = randomDatasource();
        String ip = NetworkAddress.format(randomIp(false));
        Map<String, Object> expectedGeoData = Map.of("city", "Seattle");
        when(geoIpDataDao.getGeoIpData(datasource.currentIndexName(), ip)).thenReturn(expectedGeoData);

        // Run
        Map<String, Object> geoData = ip2GeoCachedDao.getGeoData(datasource.currentIndexName(), ip, datasource.getName());

        // Verify
        assertEquals(expectedGeoData, geoData);
        // Verify datasource refresh is not triggered
        verify(datasourceDao, times(0)).getDatasource(any());
    }

    public void testGetGeoData_whenFailed_thenException() throws IOException {
        Datasource datasource = randomDatasource();
        String ip = NetworkAddress.format(randomIp(false));
        when(geoIpDataDao.getGeoIpData(datasource.currentIndexName(), ip)).thenThrow(new RuntimeException("error"));

        // Run
        assertThrows(RuntimeException.class, () -> ip2GeoCachedDao.getGeoData(datasource.currentIndexName(), ip, datasource.getName()));

        // Verify datasource refresh is triggered
        verify(datasourceDao, times(1)).getDatasource(any());
    }

    @SneakyThrows
    public void testPostIndex_whenFailed_thenResetMetadataToForcePullDataFromIndex() {
        Datasource datasource = randomDatasource();

        // At the beginning we don't have the new datasource in the system index and the cache metadata
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        // Verify we don't have the new datasource
        assertFalse(ip2GeoCachedDao.has(datasource.getName()));

        // Mock the new datasource is added to the system index
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);

        ShardId shardId = mock(ShardId.class);
        Engine.Index index = mock(Engine.Index.class);
        BytesReference bytesReference = BytesReference.bytes(datasource.toXContent(XContentFactory.jsonBuilder(), null));
        when(index.source()).thenReturn(bytesReference);
        Engine.IndexResult result = mock(Engine.IndexResult.class);
        when(result.getResultType()).thenReturn(Engine.Result.Type.FAILURE);

        // Run
        ip2GeoCachedDao.postIndex(shardId, index, result);

        // Verify
        assertTrue(ip2GeoCachedDao.has(datasource.getName()));
        assertEquals(datasource.currentIndexName(), ip2GeoCachedDao.getIndexName(datasource.getName()));
        assertEquals(datasource.getState(), ip2GeoCachedDao.getState(datasource.getName()));
    }

    @SneakyThrows
    public void testPostIndex_whenException_thenResetMetadataToForcePullDataFromIndex() {
        Datasource datasource = randomDatasource();

        // At the beginning we don't have the new datasource in the system index and the cache metadata
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        // Verify we don't have the new datasource
        assertFalse(ip2GeoCachedDao.has(datasource.getName()));

        // Mock the new datasource is added to the system index
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList(datasource));
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);

        ShardId shardId = mock(ShardId.class);
        Engine.Index index = mock(Engine.Index.class);
        BytesReference bytesReference = BytesReference.bytes(datasource.toXContent(XContentFactory.jsonBuilder(), null));
        when(index.source()).thenReturn(bytesReference);

        // Run
        ip2GeoCachedDao.postIndex(shardId, index, new Exception());

        // Verify
        assertTrue(ip2GeoCachedDao.has(datasource.getName()));
        assertEquals(datasource.currentIndexName(), ip2GeoCachedDao.getIndexName(datasource.getName()));
        assertEquals(datasource.getState(), ip2GeoCachedDao.getState(datasource.getName()));
    }

    @SneakyThrows
    public void testPostIndex_whenSucceed_thenUpdate() {
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        Datasource datasource = randomDatasource();
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);

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

    public void testPostDelete_whenFailed_thenResetMetadataToForcePullDataFromIndex() {
        Datasource datasource = randomDatasource();

        // At the beginning we don't have the new datasource in the system index and the cache metadata
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        // Verify we don't have the new datasource
        assertFalse(ip2GeoCachedDao.has(datasource.getName()));

        // Mock the new datasource is added to the system index
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

    public void testPostDelete_whenException_thenResetMetadataToForcePullDataFromIndex() {
        Datasource datasource = randomDatasource();

        // At the beginning we don't have the new datasource in the system index and the cache metadata
        when(datasourceDao.getAllDatasources()).thenReturn(Arrays.asList());
        // Verify we don't have the new datasource
        assertFalse(ip2GeoCachedDao.has(datasource.getName()));

        // Mock the new datasource is added to the system index
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
