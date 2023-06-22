/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.dao;

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
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.Strings;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.geospatial.shared.Constants;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

@SuppressForbidden(reason = "unit test")
public class GeoIpDataDaoTests extends Ip2GeoTestCase {
    private static final String IP_RANGE_FIELD_NAME = "_cidr";
    private static final String DATA_FIELD_NAME = "_data";
    private GeoIpDataDao noOpsGeoIpDataDao;
    private GeoIpDataDao verifyingGeoIpDataDao;

    @Before
    public void init() {
        noOpsGeoIpDataDao = new GeoIpDataDao(clusterService, client);
        verifyingGeoIpDataDao = new GeoIpDataDao(clusterService, verifyingClient);
    }

    public void testCreateIndexIfNotExistsWithExistingIndex() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        when(metadata.hasIndex(index)).thenReturn(true);
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> { throw new RuntimeException("Shouldn't get called"); });
        verifyingGeoIpDataDao.createIndexIfNotExists(index);
    }

    public void testCreateIndexIfNotExistsWithoutExistingIndex() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        when(metadata.hasIndex(index)).thenReturn(false);
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof CreateIndexRequest);
            CreateIndexRequest request = (CreateIndexRequest) actionRequest;
            assertEquals(index, request.index());
            assertEquals(1, (int) request.settings().getAsInt("index.number_of_shards", 0));
            assertNull(request.settings().get("index.auto_expand_replicas"));
            assertEquals(0, (int) request.settings().getAsInt("index.number_of_replicas", 1));
            assertEquals(-1, (int) request.settings().getAsInt("index.refresh_interval", 0));
            assertEquals(true, request.settings().getAsBoolean("index.hidden", false));

            assertEquals(
                "{\"dynamic\": false,\"properties\": {\"_cidr\": {\"type\": \"ip_range\",\"doc_values\": false}}}",
                request.mappings()
            );
            return null;
        });
        verifyingGeoIpDataDao.createIndexIfNotExists(index);
    }

    @SneakyThrows
    public void testCreateDocument_whenBlankValue_thenDoNotAdd() {
        String[] names = { "ip", "country", "location", "city" };
        String[] values = { "1.0.0.0/25", "USA", " ", "Seattle" };
        assertEquals(
            "{\"_cidr\":\"1.0.0.0/25\",\"_data\":{\"country\":\"USA\",\"city\":\"Seattle\"}}",
            Strings.toString(noOpsGeoIpDataDao.createDocument(names, values))
        );
    }

    @SneakyThrows
    public void testCreateDocument_whenFieldsAndValuesLengthDoesNotMatch_thenThrowException() {
        String[] names = { "ip", "country", "location", "city" };
        String[] values = { "1.0.0.0/25", "USA", " " };

        // Run
        Exception e = expectThrows(OpenSearchException.class, () -> noOpsGeoIpDataDao.createDocument(names, values));

        // Verify
        assertTrue(e.getMessage().contains("does not match"));
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
        CSVParser parser = noOpsGeoIpDataDao.getDatabaseReader(manifest);
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
        OpenSearchException exception = expectThrows(OpenSearchException.class, () -> noOpsGeoIpDataDao.getDatabaseReader(manifest));
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
        noOpsGeoIpDataDao.internalGetDatabaseReader(manifest, connection);

        // Verify
        verify(connection).addRequestProperty(Constants.USER_AGENT_KEY, Constants.USER_AGENT_VALUE);
    }

    public void testDeleteIp2GeoDataIndex_whenCalled_thenDeleteIndex() {
        String index = String.format(Locale.ROOT, "%s.%s", IP2GEO_DATA_INDEX_NAME_PREFIX, GeospatialTestHelper.randomLowerCaseString());
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof DeleteIndexRequest);
            DeleteIndexRequest request = (DeleteIndexRequest) actionRequest;
            assertEquals(1, request.indices().length);
            assertEquals(index, request.indices()[0]);
            return new AcknowledgedResponse(true);
        });
        verifyingGeoIpDataDao.deleteIp2GeoDataIndex(index);
    }

    public void testDeleteIp2GeoDataIndexWithNonIp2GeoDataIndex() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        Exception e = expectThrows(OpenSearchException.class, () -> verifyingGeoIpDataDao.deleteIp2GeoDataIndex(index));
        assertTrue(e.getMessage().contains("not ip2geo data index"));
        verify(verifyingClient, never()).index(any());
    }

    @SneakyThrows
    public void testPutGeoIpData_whenValidInput_thenSucceed() {
        String index = GeospatialTestHelper.randomLowerCaseString();
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            if (actionRequest instanceof BulkRequest) {
                BulkRequest request = (BulkRequest) actionRequest;
                assertEquals(2, request.numberOfActions());
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
                assertEquals(true, request.settings().getAsBoolean("index.blocks.write", false));
                assertNull(request.settings().get("index.num_of_replica"));
                assertEquals("0-all", request.settings().get("index.auto_expand_replicas"));
                return null;
            } else {
                throw new RuntimeException("invalid request is called");
            }
        });
        Runnable renewLock = mock(Runnable.class);
        try (CSVParser csvParser = CSVParser.parse(sampleIp2GeoFile(), StandardCharsets.UTF_8, CSVFormat.RFC4180)) {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            String[] fields = iterator.next().values();
            verifyingGeoIpDataDao.putGeoIpData(index, fields, iterator, renewLock);
            verify(renewLock, times(2)).run();
        }
    }

    public void testGetGeoIpData_whenDataExist_thenReturnTheData() {
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

        // Run
        Map<String, Object> geoData = verifyingGeoIpDataDao.getGeoIpData(indexName, ip);

        // Verify
        assertEquals("seattle", geoData.get("city"));
    }

    public void testGetGeoIpData_whenNoData_thenReturnEmpty() {
        String indexName = GeospatialTestHelper.randomLowerCaseString();
        String ip = randomIpAddress();
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assert actionRequest instanceof SearchRequest;
            SearchRequest request = (SearchRequest) actionRequest;
            assertEquals("_local", request.preference());
            assertEquals(1, request.source().size());
            assertEquals(QueryBuilders.termQuery(IP_RANGE_FIELD_NAME, ip), request.source().query());

            SearchHit[] searchHitArray = {};
            SearchHits searchHits = new SearchHits(searchHitArray, new TotalHits(0l, TotalHits.Relation.EQUAL_TO), 0);

            SearchResponse response = mock(SearchResponse.class);
            when(response.getHits()).thenReturn(searchHits);
            return response;
        });

        // Run
        Map<String, Object> geoData = verifyingGeoIpDataDao.getGeoIpData(indexName, ip);

        // Verify
        assertTrue(geoData.isEmpty());
    }
}
