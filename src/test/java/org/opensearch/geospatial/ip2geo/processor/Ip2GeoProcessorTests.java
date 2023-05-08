/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.processor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
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
        factory = new Ip2GeoProcessor.Factory(ingestService, datasourceFacade, geoIpDataFacade);
    }

    public void testCreateWithNoDatasource() {
        Map<String, Object> config = new HashMap<>();
        config.put("field", "ip");
        config.put(CONFIG_DATASOURCE_KEY, "no_datasource");
        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> factory.create(
                Collections.emptyMap(),
                GeospatialTestHelper.randomLowerCaseString(),
                GeospatialTestHelper.randomLowerCaseString(),
                config
            )
        );
        assertTrue(exception.getDetailedMessage().contains("doesn't exist"));
    }

    public void testCreateWithInvalidDatasourceState() {
        Datasource datasource = new Datasource();
        datasource.setName(GeospatialTestHelper.randomLowerCaseString());
        datasource.setState(randomStateExcept(DatasourceState.AVAILABLE));
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> createProcessor(datasource, Collections.emptyMap()));
        assertTrue(exception.getDetailedMessage().contains("available state"));
    }

    public void testCreateWithInvalidProperties() {
        Map<String, Object> config = new HashMap<>();
        config.put("properties", Arrays.asList("ip", "invalid_property"));
        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> createProcessor(GeospatialTestHelper.randomLowerCaseString(), config)
        );
        assertTrue(exception.getDetailedMessage().contains("property"));
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

    public void testExecuteWithNoIp() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Map<String, Object> config = new HashMap<>();
        Ip2GeoProcessor processor = createProcessor(datasourceName, config);
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = (doc, e) -> {};
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(document, handler));
        assertTrue(exception.getMessage().contains("not present"));
    }

    public void testExecuteWithNonStringValue() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        Map<String, Object> source = new HashMap<>();
        source.put("ip", Randomness.get().nextInt());
        IngestDocument document = new IngestDocument(source, new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = (doc, e) -> {};
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> processor.execute(document, handler));
        assertTrue(exception.getMessage().contains("string"));
    }

    public void testExecuteWithNullDatasource() throws Exception {
        BiConsumer<IngestDocument, Exception> handler = (doc, e) -> {
            assertNull(doc);
            assertTrue(e.getMessage().contains("datasource does not exist"));
        };
        getActionListener(Collections.emptyMap(), handler).onResponse(null);
    }

    public void testExecuteWithExpiredDatasource() throws Exception {
        Datasource datasource = mock(Datasource.class);
        when(datasource.isExpired()).thenReturn(true);
        BiConsumer<IngestDocument, Exception> handler = (doc, e) -> {
            assertEquals("ip2geo_data_expired", doc.getFieldValue(DEFAULT_TARGET_FIELD + ".error", String.class));
            assertNull(e);
        };
        getActionListener(Collections.emptyMap(), handler).onResponse(datasource);
    }

    public void testExecute() throws Exception {
        Map<String, Object> ip2geoData = new HashMap<>();
        for (String field : SUPPORTED_FIELDS) {
            ip2geoData.put(field, GeospatialTestHelper.randomLowerCaseString());
        }

        Datasource datasource = mock(Datasource.class);
        when(datasource.isExpired()).thenReturn(false);
        when(datasource.currentIndexName()).thenReturn(GeospatialTestHelper.randomLowerCaseString());
        BiConsumer<IngestDocument, Exception> handler = (doc, e) -> {
            assertEquals(
                ip2geoData.get(SUPPORTED_FIELDS.get(0)),
                doc.getFieldValue(DEFAULT_TARGET_FIELD + "." + SUPPORTED_FIELDS.get(0), String.class)
            );
            for (int i = 1; i < SUPPORTED_FIELDS.size(); i++) {
                assertNull(doc.getFieldValue(DEFAULT_TARGET_FIELD + "." + SUPPORTED_FIELDS.get(i), String.class, true));
            }
            assertNull(e);
        };
        Map<String, Object> config = Map.of("properties", Arrays.asList(SUPPORTED_FIELDS.get(0)));
        getActionListener(config, handler).onResponse(datasource);

        ArgumentCaptor<ActionListener<Map<String, Object>>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(geoIpDataFacade).getGeoIpData(anyString(), anyString(), captor.capture());
        captor.getValue().onResponse(ip2geoData);
    }

    private ActionListener<Datasource> getActionListener(
        final Map<String, Object> config,
        final BiConsumer<IngestDocument, Exception> handler
    ) throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, config);

        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());

        processor.execute(document, handler);
        ArgumentCaptor<ActionListener<Datasource>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceFacade).getDatasource(eq(datasourceName), captor.capture());
        return captor.getValue();
    }

    public void testExecuteNotImplemented() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        IngestDocument document = new IngestDocument(Collections.emptyMap(), Collections.emptyMap());
        Exception e = expectThrows(IllegalStateException.class, () -> processor.execute(document));
        assertTrue(e.getMessage().contains("Not implemented"));
    }

    public void testGenerateDataToAppendWithFirstOnlyOption() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(
            datasourceName,
            Map.of("first_only", true, "properties", Arrays.asList(SUPPORTED_FIELDS.get(0)))
        );
        List<String> ips = new ArrayList<>();
        Map<String, Map<String, Object>> data = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            String ip = randomIpAddress();
            ips.add(ip);
            Map<String, Object> geoData = new HashMap<>();
            for (String field : SUPPORTED_FIELDS) {
                geoData.put(field, GeospatialTestHelper.randomLowerCaseString());
            }
            data.put(ip, i == 0 ? Collections.emptyMap() : geoData);
        }
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.listenerToAppendDataToDocument(data, ips, document, handler).onResponse(data);

        // Verify
        verify(handler).accept(document, null);
        assertEquals(1, document.getFieldValue(DEFAULT_TARGET_FIELD, Map.class).size());
        assertEquals(
            data.get(ips.get(1)).get(SUPPORTED_FIELDS.get(0)),
            document.getFieldValue(DEFAULT_TARGET_FIELD, Map.class).get(SUPPORTED_FIELDS.get(0))
        );
        assertNull(document.getFieldValue(DEFAULT_TARGET_FIELD, Map.class).get(SUPPORTED_FIELDS.get(1)));
    }

    public void testGenerateDataToAppendWithOutFirstOnlyOption() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(
            datasourceName,
            Map.of("first_only", false, "properties", Arrays.asList(SUPPORTED_FIELDS.get(0)))
        );
        List<String> ips = new ArrayList<>();
        Map<String, Map<String, Object>> data = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            String ip = randomIpAddress();
            ips.add(ip);
            Map<String, Object> geoData = new HashMap<>();
            for (String field : SUPPORTED_FIELDS) {
                geoData.put(field, GeospatialTestHelper.randomLowerCaseString());
            }
            data.put(ip, i == 0 ? Collections.emptyMap() : geoData);
        }
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);

        // Run
        processor.listenerToAppendDataToDocument(data, ips, document, handler).onResponse(data);

        // Verify
        verify(handler).accept(document, null);
        assertEquals(ips.size(), document.getFieldValue(DEFAULT_TARGET_FIELD, List.class).size());
        for (int i = 0; i < ips.size(); i++) {
            if (data.get(ips.get(i)).isEmpty()) {
                assertNull(document.getFieldValue(DEFAULT_TARGET_FIELD, List.class).get(i));
            } else {
                Map<String, Object> documentData = (Map<String, Object>) document.getFieldValue(DEFAULT_TARGET_FIELD, List.class).get(i);
                assertEquals(1, documentData.size());
                assertEquals(data.get(ips.get(i)).get(SUPPORTED_FIELDS.get(0)), documentData.get(SUPPORTED_FIELDS.get(0)));
                assertNull(documentData.get(SUPPORTED_FIELDS.get(1)));
            }
        }
    }

    public void testGenerateDataToAppendWithNoData() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Map.of("first_only", Randomness.get().nextInt() % 2 == 0));
        List<String> ips = new ArrayList<>();
        Map<String, Map<String, Object>> data = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            String ip = randomIpAddress();
            ips.add(ip);
            data.put(ip, Collections.emptyMap());
        }
        IngestDocument document = new IngestDocument(new HashMap<>(), new HashMap<>());
        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);
        processor.listenerToAppendDataToDocument(data, ips, document, handler).onResponse(data);
        verify(handler).accept(document, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> document.getFieldValue(DEFAULT_TARGET_FIELD, Map.class));
        assertTrue(e.getMessage().contains("not present"));
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

    public void testExecuteInternal() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Ip2GeoProcessor processor = createProcessor(datasourceName, Collections.emptyMap());
        List<?> ips = Arrays.asList(randomIpAddress(), randomIpAddress());
        Map<String, Object> source = new HashMap<>();
        String ip = randomIpAddress();
        source.put("ip", ip);
        IngestDocument document = new IngestDocument(source, new HashMap<>());

        BiConsumer<IngestDocument, Exception> handler = mock(BiConsumer.class);
        processor.executeInternal(document, handler, ips);

        ArgumentCaptor<ActionListener<Datasource>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceFacade).getDatasource(eq(datasourceName), captor.capture());
        Datasource datasource = mock(Datasource.class);
        when(datasource.isExpired()).thenReturn(false);
        when(datasource.currentIndexName()).thenReturn(GeospatialTestHelper.randomLowerCaseString());
        captor.getValue().onResponse(datasource);
        verify(geoIpDataFacade).getGeoIpData(
            anyString(),
            any(Iterator.class),
            anyInt(),
            anyInt(),
            anyBoolean(),
            anyMap(),
            any(ActionListener.class)
        );
    }

    private Ip2GeoProcessor createProcessor(final String datasourceName, final Map<String, Object> config) throws Exception {
        Datasource datasource = new Datasource();
        datasource.setName(datasourceName);
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.getDatabase().setFields(SUPPORTED_FIELDS);
        return createProcessor(datasource, config);
    }

    private Ip2GeoProcessor createProcessor(final Datasource datasource, final Map<String, Object> config) throws Exception {
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
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
