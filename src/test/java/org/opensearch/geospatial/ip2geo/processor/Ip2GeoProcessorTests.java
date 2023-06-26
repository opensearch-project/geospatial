/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.common.Randomness;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.ingest.IngestDocument;

public class Ip2GeoProcessorTests extends Ip2GeoTestCase {
    private static final String DEFAULT_TARGET_FIELD = "ip2geo";
    private static final String CONFIG_DATASOURCE_KEY = "datasource";
    private static final String CONFIG_FIELD_KEY = "field";
    private static final List<String> SUPPORTED_FIELDS = Arrays.asList("city", "country");
    private Ip2GeoProcessor.Factory factory;

    @Before
    public void init() {
        factory = new Ip2GeoProcessor.Factory(ingestService, datasourceDao, geoIpDataDao, ip2GeoCache);
    }

    public void testExecuteWithNoIpAndIgnoreMissing() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Map<String, Object> config = new HashMap<>();
        config.put("ignore_missing", true);
        Ip2GeoProcessor processor = createProcessor(datasourceName, config);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = (doc, e) -> {
            assertEquals(document, doc);
            assertNull(e);
        };
        processor.execute(document, handler);
    }

    public void testExecute_whenNoIp_thenException() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Map<String, Object> config = new HashMap<>();
        Ip2GeoProcessor processor = createProcessor(datasourceName, config);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.execute(document, handler);

        // Verify
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_whenNonStringValue_thenException() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        source.put("ip", Randomness.get().nextInt());
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.execute(document, handler);

        // Verify
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    @SneakyThrows
    public void testExecute_whenNoDatasource_thenNotExistError() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());

        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());

        when(ip2GeoCache.has(datasourceName)).thenReturn(false);
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.execute(document, handler);

        // Verify
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(Exception.class);
        verify(handler).accept(isNull(), captor.capture());
        captor.getValue().getMessage().contains("not exist");
    }

    @SneakyThrows
    public void testExecute_whenExpired_thenExpiredMsg() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());

        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());

        String index = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCache.getIndexName(datasourceName)).thenReturn(index);
        when(ip2GeoCache.has(datasourceName)).thenReturn(true);
        when(ip2GeoCache.isExpired(datasourceName)).thenReturn(true);
        when(ip2GeoCache.getState(datasourceName)).thenReturn(DatasourceState.AVAILABLE);

        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.execute(document, handler);

        // Verify
        verify(handler).accept(document, null);
        assertEquals("ip2geo_data_expired", document.getFieldValue(DEFAULT_TARGET_FIELD + ".error", String.class));
    }

    @SneakyThrows
    public void testExecuteInternal_whenSingleIp_thenGetDatasourceIsCalled() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        String indexName = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCache.getIndexName(datasourceName)).thenReturn(indexName);
        when(ip2GeoCache.has(datasourceName)).thenReturn(true);
        when(ip2GeoCache.getState(datasourceName)).thenReturn(DatasourceState.AVAILABLE);
        when(ip2GeoCache.isExpired(datasourceName)).thenReturn(false);

        // Run
        processor.executeInternal(document, handler, ip);

        // Verify
        verify(geoIpDataDao).getGeoIpData(anyString(), anyString(), any(ActionListener.class));
    }

    @SneakyThrows
    public void testGetSingleGeoIpDataListener_whenNoPropertySet_thenAddAllProperties() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        Map<String, Object> geoIpData = Map.of("city", "Seattle", "country", "USA");
        // Run
        processor.getSingleGeoIpDataListener(document, handler).onResponse(geoIpData);

        // Verify
        assertEquals("Seattle", document.getFieldValue(DEFAULT_TARGET_FIELD + ".city", String.class));
        assertEquals("USA", document.getFieldValue(DEFAULT_TARGET_FIELD + ".country", String.class));
        verify(handler).accept(document, null);
    }

    @SneakyThrows
    public void testGetSingleGeoIpDataListener_whenPropertySet_thenAddOnlyTheProperties() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Map.of("properties", Arrays.asList("city")));
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        Map<String, Object> geoIpData = Map.of("city", "Seattle", "country", "USA");
        // Run
        processor.getSingleGeoIpDataListener(document, handler).onResponse(geoIpData);

        // Verify
        assertEquals("Seattle", document.getFieldValue(DEFAULT_TARGET_FIELD + ".city", String.class));
        assertFalse(document.hasField(DEFAULT_TARGET_FIELD + ".country"));
        verify(handler).accept(document, null);
    }

    @SneakyThrows
    public void testGetMultiGeoIpDataListener_whenNoPropertySet_thenAddAllProperties() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        Map<String, Object> geoIpData = Map.of("city", "Seattle", "country", "USA");
        // Run
        processor.getMultiGeoIpDataListener(document, handler).onResponse(Arrays.asList(geoIpData));

        // Verify
        assertEquals(1, document.getFieldValue(DEFAULT_TARGET_FIELD, List.class).size());
        assertEquals("Seattle", document.getFieldValue(DEFAULT_TARGET_FIELD + ".0.city", String.class));
        assertEquals("USA", document.getFieldValue(DEFAULT_TARGET_FIELD + ".0.country", String.class));
        verify(handler).accept(document, null);
    }

    @SneakyThrows
    public void testGetMultiGeoIpDataListener_whenPropertySet_thenAddOnlyTheProperties() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Map.of("properties", Arrays.asList("city")));
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        Map<String, Object> geoIpData = Map.of("city", "Seattle", "country", "USA");
        // Run
        processor.getMultiGeoIpDataListener(document, handler).onResponse(Arrays.asList(geoIpData));

        // Verify
        assertEquals(1, document.getFieldValue(DEFAULT_TARGET_FIELD, List.class).size());
        assertEquals("Seattle", document.getFieldValue(DEFAULT_TARGET_FIELD + ".0.city", String.class));
        assertFalse(document.hasField(DEFAULT_TARGET_FIELD + ".0.country"));
        verify(handler).accept(document, null);
    }

    public void testExecuteNotImplemented() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        IngestDocument document = new IngestDocument(Collections.emptyMap(), Collections.emptyMap());
        Exception e = expectThrows(IllegalStateException.class, () -> processor.execute(document));
        assertTrue(e.getMessage().contains("Not implemented"));
    }

    public void testExecuteInternalNonStringIp() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        List<?> ips = Arrays.asList(randomIpAddress(), 1);
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());

        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);
        Exception e = expectThrows(IllegalArgumentException.class, () -> processor.executeInternal(document, handler, ips));
        assertTrue(e.getMessage().contains("should only contain strings"));
    }

    @SneakyThrows
    public void testExecuteInternal_whenMultiIps_thenGetDatasourceIsCalled() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);
        List<?> ips = Arrays.asList(randomIpAddress(), randomIpAddress());

        String indexName = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCache.getIndexName(datasourceName)).thenReturn(indexName);
        when(ip2GeoCache.has(datasourceName)).thenReturn(true);
        when(ip2GeoCache.getState(datasourceName)).thenReturn(DatasourceState.AVAILABLE);
        when(ip2GeoCache.isExpired(datasourceName)).thenReturn(false);

        // Run
        processor.executeInternal(document, handler, ips);

        // Verify
        verify(geoIpDataDao).getGeoIpData(anyString(), anyList(), any(ActionListener.class));
    }

    private Ip2GeoProcessor createProcessor(final String datasourceName, final Map<String, Object> config) throws Exception {
        Datasource datasource = new Datasource();
        datasource.setName(datasourceName);
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.getDatabase().setFields(SUPPORTED_FIELDS);
        return createProcessor(datasource, config);
    }

    private Ip2GeoProcessor createProcessor(final Datasource datasource, final Map<String, Object> config) throws Exception {
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        Map<String, Object> baseConfig = new HashMap<>();
        baseConfig.put(CONFIG_FIELD_KEY, "ip");
        baseConfig.put(CONFIG_DATASOURCE_KEY, datasource.getName());
        baseConfig.putAll(config);

        return factory.create(
            Collections.emptyMap(),
            GeospatialTestHelper.randomLowerCaseString(),
            GeospatialTestHelper.randomLowerCaseString(),
            baseConfig
        );
    }
}
