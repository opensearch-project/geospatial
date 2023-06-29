/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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
import org.opensearch.common.Randomness;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.ingest.IngestDocument;

public class Ip2GeoProcessorTests extends Ip2GeoTestCase {
    private static final String DEFAULT_TARGET_FIELD = "ip2geo";
    private static final List<String> SUPPORTED_FIELDS = Arrays.asList("city", "country");
    private Ip2GeoProcessor.Factory factory;

    @Before
    public void init() {
        factory = new Ip2GeoProcessor.Factory(ingestService, datasourceDao, geoIpDataDao, ip2GeoCachedDao);
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

        when(ip2GeoCachedDao.has(datasourceName)).thenReturn(false);
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
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        String indexName = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCachedDao.getIndexName(datasourceName)).thenReturn(indexName);
        when(ip2GeoCachedDao.has(datasourceName)).thenReturn(true);
        when(ip2GeoCachedDao.getState(datasourceName)).thenReturn(DatasourceState.AVAILABLE);
        when(ip2GeoCachedDao.isExpired(datasourceName)).thenReturn(true);
        Map<String, Object> geoData = Map.of("city", "Seattle", "country", "USA");
        when(ip2GeoCachedDao.getGeoData(eq(indexName), any())).thenReturn(geoData);

        // Run for single ip
        String ip = randomIpAddress();
        IngestDocument documentWithIp = createDocument(ip);
        processor.execute(documentWithIp, handler);

        // Verify
        verify(handler).accept(documentWithIp, null);
        assertEquals("ip2geo_data_expired", documentWithIp.getFieldValue(DEFAULT_TARGET_FIELD + ".error", String.class));

        // Run for multi ips
        List<String> ips = Arrays.asList(randomIpAddress(), randomIpAddress());
        IngestDocument documentWithIps = createDocument(ips);
        processor.execute(documentWithIps, handler);

        // Verify
        verify(handler).accept(documentWithIps, null);
        assertEquals("ip2geo_data_expired", documentWithIp.getFieldValue(DEFAULT_TARGET_FIELD + ".error", String.class));
    }

    @SneakyThrows
    public void testExecute_whenNotAvailable_thenException() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        String indexName = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCachedDao.getIndexName(datasourceName)).thenReturn(indexName);
        when(ip2GeoCachedDao.has(datasourceName)).thenReturn(true);
        when(ip2GeoCachedDao.getState(datasourceName)).thenReturn(DatasourceState.CREATE_FAILED);
        when(ip2GeoCachedDao.isExpired(datasourceName)).thenReturn(false);
        Map<String, Object> geoData = Map.of("city", "Seattle", "country", "USA");
        when(ip2GeoCachedDao.getGeoData(eq(indexName), any())).thenReturn(geoData);

        // Run for single ip
        String ip = randomIpAddress();
        IngestDocument documentWithIp = createDocument(ip);
        processor.execute(documentWithIp, handler);

        // Run for multi ips
        List<String> ips = Arrays.asList(randomIpAddress(), randomIpAddress());
        IngestDocument documentWithIps = createDocument(ips);
        processor.execute(documentWithIps, handler);

        // Verify
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(IllegalStateException.class);
        verify(handler, times(2)).accept(isNull(), captor.capture());
        assertTrue(captor.getAllValues().stream().allMatch(e -> e.getMessage().contains("not in an available state")));
    }

    @SneakyThrows
    public void testExecute_whenCalled_thenGeoIpDataIsAdded() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        String indexName = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCachedDao.getIndexName(datasourceName)).thenReturn(indexName);
        when(ip2GeoCachedDao.has(datasourceName)).thenReturn(true);
        when(ip2GeoCachedDao.getState(datasourceName)).thenReturn(DatasourceState.AVAILABLE);
        when(ip2GeoCachedDao.isExpired(datasourceName)).thenReturn(false);
        Map<String, Object> geoData = Map.of("city", "Seattle", "country", "USA");
        when(ip2GeoCachedDao.getGeoData(eq(indexName), any())).thenReturn(geoData);

        // Run for single ip
        String ip = randomIpAddress();
        IngestDocument documentWithIp = createDocument(ip);
        processor.execute(documentWithIp, handler);

        // Verify
        assertEquals(geoData.get("city"), documentWithIp.getFieldValue("ip2geo.city", String.class));
        assertEquals(geoData.get("country"), documentWithIp.getFieldValue("ip2geo.country", String.class));

        // Run for multi ips
        List<String> ips = Arrays.asList(randomIpAddress(), randomIpAddress());
        IngestDocument documentWithIps = createDocument(ips);
        processor.execute(documentWithIps, handler);

        // Verify
        assertEquals(2, documentWithIps.getFieldValue("ip2geo", List.class).size());
        Map<String, Object> addedValue = (Map<String, Object>) documentWithIps.getFieldValue("ip2geo", List.class).get(0);
        assertEquals(geoData.get("city"), addedValue.get("city"));
        assertEquals(geoData.get("country"), addedValue.get("country"));
    }

    @SneakyThrows
    public void testExecute_whenPropertiesSet_thenFilteredGeoIpDataIsAdded() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Map.of(Ip2GeoProcessor.CONFIG_PROPERTIES, Arrays.asList("country")));
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        String indexName = GeospatialTestHelper.randomLowerCaseString();
        when(ip2GeoCachedDao.getIndexName(datasourceName)).thenReturn(indexName);
        when(ip2GeoCachedDao.has(datasourceName)).thenReturn(true);
        when(ip2GeoCachedDao.getState(datasourceName)).thenReturn(DatasourceState.AVAILABLE);
        when(ip2GeoCachedDao.isExpired(datasourceName)).thenReturn(false);
        Map<String, Object> geoData = Map.of("city", "Seattle", "country", "USA");
        when(ip2GeoCachedDao.getGeoData(eq(indexName), any())).thenReturn(geoData);

        // Run for single ip
        String ip = randomIpAddress();
        IngestDocument documentWithIp = createDocument(ip);
        processor.execute(documentWithIp, handler);

        // Verify
        assertFalse(documentWithIp.hasField("ip2geo.city"));
        assertEquals(geoData.get("country"), documentWithIp.getFieldValue("ip2geo.country", String.class));

        // Run for multi ips
        List<String> ips = Arrays.asList(randomIpAddress(), randomIpAddress());
        IngestDocument documentWithIps = createDocument(ips);
        processor.execute(documentWithIps, handler);

        // Verify
        assertEquals(2, documentWithIps.getFieldValue("ip2geo", List.class).size());
        Map<String, Object> addedValue = (Map<String, Object>) documentWithIps.getFieldValue("ip2geo", List.class).get(0);
        assertFalse(addedValue.containsKey("city"));
        assertEquals(geoData.get("country"), addedValue.get("country"));
    }

    public void testExecute_whenNoHandler_thenException() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        IngestDocument document = new IngestDocument(Collections.emptyMap(), Collections.emptyMap());
        Exception e = expectThrows(IllegalStateException.class, () -> processor.execute(document));
        assertTrue(e.getMessage().contains("Not implemented"));
    }

    public void testExecute_whenContainsNonString_thenException() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        List<?> ips = Arrays.asList(randomIpAddress(), 1);
        Map<String, Object> source = new HashMap<>();
        source.put("ip", ips);
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.execute(document, handler);

        // Verify
        ArgumentCaptor<Exception> captor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(handler).accept(isNull(), captor.capture());
        assertTrue(captor.getValue().getMessage().contains("should only contain strings"));
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
        baseConfig.put(Ip2GeoProcessor.CONFIG_FIELD, "ip");
        baseConfig.put(Ip2GeoProcessor.CONFIG_DATASOURCE, datasource.getName());
        baseConfig.putAll(config);

        return factory.create(
            Collections.emptyMap(),
            GeospatialTestHelper.randomLowerCaseString(),
            GeospatialTestHelper.randomLowerCaseString(),
            baseConfig
        );
    }

    private IngestDocument createDocument(String ip) {
        Map<String, Object> source = new HashMap<>();
        source.put("ip", ip);
        return new IngestDocument(source, new HashMap<>());
    }

    private IngestDocument createDocument(List<String> ips) {
        Map<String, Object> source = new HashMap<>();
        source.put("ip", ips);
        return new IngestDocument(source, new HashMap<>());
    }
}
