/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.Randomness;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.shared.Constants;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

@SuppressForbidden(reason = "unit test")
public class GeoIpDataFacadeTests extends Ip2GeoTestCase {
    private static final String IP_RANGE_FIELD_NAME = "_cidr";
    private static final String DATA_FIELD_NAME = "_data";
    private GeoIpDataFacade noOpsGeoIpDataFacade;
    private GeoIpDataFacade verifyingGeoIpDataFacade;

    @Before
    public void init() {
        noOpsGeoIpDataFacade = new GeoIpDataFacade(clusterService, client);
        verifyingGeoIpDataFacade = new GeoIpDataFacade(clusterService, verifyingClient);
    }

    public void testCreateIndexIfNotExistsWithExistingIndex() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        when(metadata.hasIndex(index)).thenReturn(true);
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> { throw new RuntimeException("Shouldn't get called"); });
        verifyingGeoIpDataFacade.createIndexIfNotExists(index);
    }

    public void testCreateIndexIfNotExistsWithoutExistingIndex() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        when(metadata.hasIndex(index)).thenReturn(false);
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof CreateIndexRequest);
            CreateIndexRequest request = (CreateIndexRequest) actionRequest;
            assertEquals(index, request.index());
            assertEquals(1, (int) request.settings().getAsInt("index.number_of_shards", 2));
            assertEquals("0-all", request.settings().get("index.auto_expand_replicas"));
            assertEquals(true, request.settings().getAsBoolean("index.hidden", false));

            assertEquals(
                "{\"dynamic\": false,\"properties\": {\"_cidr\": {\"type\": \"ip_range\",\"doc_values\": false}}}",
                request.mappings()
            );
            return null;
        });
        verifyingGeoIpDataFacade.createIndexIfNotExists(index);
    }

    public void testCreateDocument() {
        String[] names = { "ip", "country", "city" };
        String[] values = { "1.0.0.0/25", "USA", "Seattle" };
        assertEquals(
            "{\"_cidr\":\"1.0.0.0/25\",\"_data\":{\"country\":\"USA\",\"city\":\"Seattle\"}}",
            noOpsGeoIpDataFacade.createDocument(names, values)
        );
    }

    public void testGetDatabaseReader() throws Exception {
        File zipFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.zip").getFile());
        DatasourceManifest manifest = new DatasourceManifest(
            zipFile.toURI().toURL().toExternalForm(),
            "sample_valid.csv",
            "fake_sha256",
            1l,
            Instant.now().toEpochMilli(),
            "tester"
        );
        CSVParser parser = noOpsGeoIpDataFacade.getDatabaseReader(manifest);
        String[] expectedHeader = { "network", "country_name" };
        assertArrayEquals(expectedHeader, parser.iterator().next().values());
        String[] expectedValues = { "1.0.0.0/24", "Australia" };
        assertArrayEquals(expectedValues, parser.iterator().next().values());
    }

    public void testGetDatabaseReaderNoFile() throws Exception {
        File zipFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.zip").getFile());
        DatasourceManifest manifest = new DatasourceManifest(
            zipFile.toURI().toURL().toExternalForm(),
            "no_file.csv",
            "fake_sha256",
            1l,
            Instant.now().toEpochMilli(),
            "tester"
        );
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> noOpsGeoIpDataFacade.getDatabaseReader(manifest));
        assertTrue(exception.getMessage().contains("does not exist"));
    }

    @SneakyThrows
    public void testInternalGetDatabaseReader_whenCalled_thenSetUserAgent() {
        File zipFile = new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.zip").getFile());
        DatasourceManifest manifest = new DatasourceManifest(
            zipFile.toURI().toURL().toExternalForm(),
            "sample_valid.csv",
            "fake_sha256",
            1l,
            Instant.now().toEpochMilli(),
            "tester"
        );

        URLConnection connection = mock(URLConnection.class);
        when(connection.getInputStream()).thenReturn(new FileInputStream(zipFile));

        // Run
        noOpsGeoIpDataFacade.internalGetDatabaseReader(manifest, connection);

        // Verify
        verify(connection).addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
    }

    public void testDeleteIp2GeoDataIndex() {
        String index = String.format(Locale.ROOT, "%s.%s", IP2GEO_DATA_INDEX_NAME_PREFIX, GeospatialTestHelper.randomLowerCaseString());
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof DeleteIndexRequest);
            DeleteIndexRequest request = (DeleteIndexRequest) actionRequest;
            assertEquals(1, request.indices().length);
            assertEquals(index, request.indices()[0]);
            assertEquals(IndicesOptions.LENIENT_EXPAND_OPEN, request.indicesOptions());
            return null;
        });
        verifyingGeoIpDataFacade.deleteIp2GeoDataIndex(index);
    }

    public void testDeleteIp2GeoDataIndexWithNonIp2GeoDataIndex() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        Exception e = expectThrows(OpenSearchException.class, () -> verifyingGeoIpDataFacade.deleteIp2GeoDataIndex(index));
        assertTrue(e.getMessage().contains("not ip2geo data index"));
        verify(verifyingClient, never()).index(any());
    }

    @SneakyThrows
    public void testPutGeoIpData_whenValidInput_thenSucceed() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            if (actionRequest instanceof BulkRequest) {
                BulkRequest request = (BulkRequest) actionRequest;
                assertEquals(1, request.numberOfActions());
                assertEquals(WriteRequest.RefreshPolicy.WAIT_UNTIL, request.getRefreshPolicy());
                BulkResponse response = mock(BulkResponse.class);
                when(response.hasFailures()).thenReturn(false);
                return response;
            } else if (actionRequest instanceof RefreshRequest) {
                RefreshRequest request = (RefreshRequest) actionRequest;
                assertEquals(1, request.indices().length);
                assertEquals(index, request.indices()[0]);
                return null;
            } else if (actionRequest instanceof ForceMergeRequest) {
                ForceMergeRequest request = (ForceMergeRequest) actionRequest;
                assertEquals(1, request.indices().length);
                assertEquals(index, request.indices()[0]);
                assertEquals(1, request.maxNumSegments());
                return null;
            } else if (actionRequest instanceof UpdateSettingsRequest) {
                UpdateSettingsRequest request = (UpdateSettingsRequest) actionRequest;
                assertEquals(1, request.indices().length);
                assertEquals(index, request.indices()[0]);
                assertEquals(true, request.settings().getAsBoolean("index.blocks.read_only_allow_delete", false));
                return null;
            } else {
                throw new RuntimeException("invalid request is called");
            }
        });
        Runnable renewLock = mock(Runnable.class);
        try (CSVParser csvParser = CSVParser.parse(sampleIp2GeoFile(), StandardCharsets.UTF_8, CSVFormat.RFC4180)) {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            String[] fields = iterator.next().values();
            verifyingGeoIpDataFacade.putGeoIpData(index, fields, iterator, 1, renewLock);
            verify(renewLock, times(2)).run();
        }
    }

    public void testGetSingleGeoIpData() {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String ip = randomIpAddress();
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assert actionRequest instanceof SearchRequest;
            SearchRequest request = (SearchRequest) actionRequest;
            assertEquals("_local", request.preference());
            assertEquals(1, request.source().size());
            assertEquals(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip), request.source().query());

            String data = String.format(
                Locale.ROOT,
                "{\"%s\":\"1.0.0.1/16\",\"%s\":{\"city\":\"seattle\"}}",
                IP_RANGE_FIELD_NAME,
                DATA_FIELD_NAME
            );
            SearchHit searchHit = new SearchHit(1);
            searchHit.sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8))));
            SearchHit[] searchHitArray = { searchHit };
            SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(1l, TotalHits.Relation.EQUAL_TO), 1);

            SearchResponse response = mock(SearchResponse.class);
            when(response.getHits()).thenReturn(searchHits);
            return response;
        });
        ActionListener<Map<String, Object>> listener = mock(ActionListener.class);
        verifyingGeoIpDataFacade.getGeoIpData(indexName, ip, listener);
        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(listener).onResponse(captor.capture());
        assertEquals("seattle", captor.getValue().get("city"));
    }

    public void testGetMultipleGeoIpDataNoSearchRequired() {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String ip1 = randomIpAddress();
        String ip2 = randomIpAddress();
        Iterator<String> ipIterator = Arrays.asList(ip1, ip2).iterator();
        int maxBundleSize = 1;
        int maxConcurrentSearches = 1;
        boolean firstOnly = true;
        Map<String, Map<String, Object>> geoData = new HashMap<>();
        geoData.put(ip1, Map.of("city", "Seattle"));
        geoData.put(ip2, Map.of("city", "Hawaii"));
        ActionListener<Map<String, Map<String, Object>>> actionListener = mock(ActionListener.class);

        // Run
        verifyingGeoIpDataFacade.getGeoIpData(
            indexName,
            ipIterator,
            maxBundleSize,
            maxConcurrentSearches,
            firstOnly,
            geoData,
            actionListener
        );

        // Verify
        verify(actionListener).onResponse(geoData);
    }

    public void testGetMultipleGeoIpData() {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        int dataSize = Randomness.get().nextInt(10) + 1;
        List<String> ips = new ArrayList<>();
        for (int i = 0; i < dataSize; i++) {
            ips.add(randomIpAddress());
        }
        int maxBundleSize = Randomness.get().nextInt(11) + 1;
        int maxConcurrentSearches = 1;
        boolean firstOnly = false;
        Map<String, Map<String, Object>> geoData = new HashMap<>();
        ActionListener<Map<String, Map<String, Object>>> actionListener = mock(ActionListener.class);

        List<String> cities = new ArrayList<>();
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assert actionRequest instanceof MultiSearchRequest;
            MultiSearchRequest request = (MultiSearchRequest) actionRequest;
            assertEquals(maxConcurrentSearches, request.maxConcurrentSearchRequests());
            assertTrue(request.requests().size() == maxBundleSize || request.requests().size() == dataSize % maxBundleSize);
            for (SearchRequest searchRequest : request.requests()) {
                assertEquals("_local", searchRequest.preference());
                assertEquals(1, searchRequest.source().size());
            }

            MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[request.requests().size()];
            for (int i = 0; i < request.requests().size(); i++) {
                String city = GeospatialTestHelper.randomLowerCaseString();
                cities.add(city);
                String data = String.format(
                    Locale.ROOT,
                    "{\"%s\":\"1.0.0.1/16\",\"%s\":{\"city\":\"%s\"}}",
                    IP_RANGE_FIELD_NAME,
                    DATA_FIELD_NAME,
                    city
                );
                SearchHit searchHit = new SearchHit(1);
                searchHit.sourceRef(BytesReference.fromByteBuffer(ByteBuffer.wrap(data.getBytes(StandardCharsets.UTF_8))));
                SearchHit[] searchHitArray = { searchHit };
                SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(1l, TotalHits.Relation.EQUAL_TO), 1);
                SearchResponse searchResponse = mock(SearchResponse.class);
                when(searchResponse.getHits()).thenReturn(searchHits);
                MultiSearchResponse.Item item = mock(MultiSearchResponse.Item.class);
                when(item.isFailure()).thenReturn(false);
                when(item.getResponse()).thenReturn(searchResponse);
                items[i] = item;
            }
            MultiSearchResponse response = mock(MultiSearchResponse.class);
            when(response.getResponses()).thenReturn(items);
            return response;
        });

        // Run
        verifyingGeoIpDataFacade.getGeoIpData(
            indexName,
            ips.iterator(),
            maxBundleSize,
            maxConcurrentSearches,
            firstOnly,
            geoData,
            actionListener
        );

        // Verify
        verify(verifyingClient, times((dataSize + maxBundleSize - 1) / maxBundleSize)).execute(
            any(ActionType.class),
            any(ActionRequest.class),
            any(ActionListener.class)
        );
        for (int i = 0; i < dataSize; i++) {
            assertEquals(cities.get(i), geoData.get(ips.get(i)).get("city"));
        }
    }
}
