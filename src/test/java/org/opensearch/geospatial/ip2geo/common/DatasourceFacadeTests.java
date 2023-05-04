/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetItemResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.common.Randomness;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

public class DatasourceFacadeTests extends Ip2GeoTestCase {
    private DatasourceFacade datasourceFacade;

    @Before
    public void init() {
        datasourceFacade = new DatasourceFacade(verifyingClient, clusterService);
    }

    public void testCreateIndexIfNotExists_whenIndexExist_thenCreateRequestIsNotCalled() {
        when(metadata.hasIndex(DatasourceExtension.JOB_INDEX_NAME)).thenReturn(true);

        // Verify
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> { throw new RuntimeException("Shouldn't get called"); });

        // Run
        StepListener<Void> stepListener = new StepListener<>();
        datasourceFacade.createIndexIfNotExists(stepListener);

        // Verify stepListener is called
        stepListener.result();
    }

    public void testCreateIndexIfNotExists_whenIndexExist_thenCreateRequestIsCalled() {
        when(metadata.hasIndex(DatasourceExtension.JOB_INDEX_NAME)).thenReturn(false);

        // Verify
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof CreateIndexRequest);
            CreateIndexRequest request = (CreateIndexRequest) actionRequest;
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            assertEquals("1", request.settings().get("index.number_of_shards"));
            assertEquals("0-all", request.settings().get("index.auto_expand_replicas"));
            assertEquals("true", request.settings().get("index.hidden"));
            assertNotNull(request.mappings());
            return null;
        });

        // Run
        StepListener<Void> stepListener = new StepListener<>();
        datasourceFacade.createIndexIfNotExists(stepListener);

        // Verify stepListener is called
        stepListener.result();
    }

    public void testCreateIndexIfNotExists_whenIndexCreatedAlready_thenExceptionIsIgnored() {
        when(metadata.hasIndex(DatasourceExtension.JOB_INDEX_NAME)).thenReturn(false);
        verifyingClient.setExecuteVerifier(
            (actionResponse, actionRequest) -> { throw new ResourceAlreadyExistsException(DatasourceExtension.JOB_INDEX_NAME); }
        );

        // Run
        StepListener<Void> stepListener = new StepListener<>();
        datasourceFacade.createIndexIfNotExists(stepListener);

        // Verify stepListener is called
        stepListener.result();
    }

    public void testCreateIndexIfNotExists_whenExceptionIsThrown_thenExceptionIsThrown() {
        when(metadata.hasIndex(DatasourceExtension.JOB_INDEX_NAME)).thenReturn(false);
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> { throw new RuntimeException(); });

        // Run
        StepListener<Void> stepListener = new StepListener<>();
        datasourceFacade.createIndexIfNotExists(stepListener);

        // Verify stepListener is called
        expectThrows(RuntimeException.class, () -> stepListener.result());
    }

    public void testUpdateDatasource_whenValidInput_thenSucceed() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource(
            datasourceName,
            new IntervalSchedule(Instant.now().truncatedTo(ChronoUnit.MILLIS), 1, ChronoUnit.DAYS),
            "https://test.com"
        );
        Instant previousTime = Instant.now().minusMillis(1);
        datasource.setLastUpdateTime(previousTime);

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof IndexRequest);
            IndexRequest request = (IndexRequest) actionRequest;
            assertEquals(datasource.getName(), request.id());
            assertEquals(DocWriteRequest.OpType.INDEX, request.opType());
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, request.getRefreshPolicy());
            return null;
        });

        datasourceFacade.updateDatasource(datasource);
        assertTrue(previousTime.isBefore(datasource.getLastUpdateTime()));
    }

    public void testGetDatasource_whenException_thenNull() throws Exception {
        Datasource datasource = setupClientForGetRequest(true, new IndexNotFoundException(DatasourceExtension.JOB_INDEX_NAME));
        assertNull(datasourceFacade.getDatasource(datasource.getName()));
    }

    public void testGetDatasource_whenExist_thenReturnDatasource() throws Exception {
        Datasource datasource = setupClientForGetRequest(true, null);
        assertEquals(datasource, datasourceFacade.getDatasource(datasource.getName()));
    }

    public void testGetDatasource_whenNotExist_thenNull() throws Exception {
        Datasource datasource = setupClientForGetRequest(false, null);
        assertNull(datasourceFacade.getDatasource(datasource.getName()));
    }

    public void testGetDatasource_whenExistWithListener_thenListenerIsCalledWithDatasource() {
        Datasource datasource = setupClientForGetRequest(true, null);
        ActionListener<Datasource> listener = mock(ActionListener.class);
        datasourceFacade.getDatasource(datasource.getName(), listener);
        verify(listener).onResponse(eq(datasource));
    }

    public void testGetDatasource_whenNotExistWithListener_thenListenerIsCalledWithNull() {
        Datasource datasource = setupClientForGetRequest(false, null);
        ActionListener<Datasource> listener = mock(ActionListener.class);
        datasourceFacade.getDatasource(datasource.getName(), listener);
        verify(listener).onResponse(null);
    }

    private Datasource setupClientForGetRequest(final boolean isExist, final RuntimeException exception) {
        Datasource datasource = randomDatasource();

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof GetRequest);
            GetRequest request = (GetRequest) actionRequest;
            assertEquals(datasource.getName(), request.id());
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            GetResponse response = getMockedGetResponse(isExist ? datasource : null);
            if (exception != null) {
                throw exception;
            }
            return response;
        });
        return datasource;
    }

    public void testGetDatasources_whenValidInput_thenSucceed() {
        List<Datasource> datasources = Arrays.asList(randomDatasource(), randomDatasource());
        String[] names = datasources.stream().map(Datasource::getName).toArray(String[]::new);
        ActionListener<List<Datasource>> listener = mock(ActionListener.class);
        MultiGetItemResponse[] multiGetItemResponses = datasources.stream().map(datasource -> {
            GetResponse getResponse = getMockedGetResponse(datasource);
            MultiGetItemResponse multiGetItemResponse = mock(MultiGetItemResponse.class);
            when(multiGetItemResponse.getResponse()).thenReturn(getResponse);
            return multiGetItemResponse;
        }).toArray(MultiGetItemResponse[]::new);

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            // Verify
            assertTrue(actionRequest instanceof MultiGetRequest);
            MultiGetRequest request = (MultiGetRequest) actionRequest;
            assertEquals(2, request.getItems().size());
            for (MultiGetRequest.Item item : request.getItems()) {
                assertEquals(DatasourceExtension.JOB_INDEX_NAME, item.index());
                assertTrue(datasources.stream().filter(datasource -> datasource.getName().equals(item.id())).findAny().isPresent());
            }

            MultiGetResponse response = mock(MultiGetResponse.class);
            when(response.getResponses()).thenReturn(multiGetItemResponses);
            return response;
        });

        // Run
        datasourceFacade.getDatasources(names, listener);

        // Verify
        ArgumentCaptor<List<Datasource>> captor = ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        assertEquals(datasources, captor.getValue());

    }

    public void testGetAllDatasources_whenValidInput_thenSucceed() {
        List<Datasource> datasources = Arrays.asList(randomDatasource(), randomDatasource());
        ActionListener<List<Datasource>> listener = mock(ActionListener.class);
        SearchHits searchHits = getMockedSearchHits(datasources);

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            // Verify
            assertTrue(actionRequest instanceof SearchRequest);
            SearchRequest request = (SearchRequest) actionRequest;
            assertEquals(1, request.indices().length);
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.indices()[0]);
            assertEquals(QueryBuilders.matchAllQuery(), request.source().query());
            assertEquals(1000, request.source().size());

            SearchResponse response = mock(SearchResponse.class);
            when(response.getHits()).thenReturn(searchHits);
            return response;
        });

        // Run
        datasourceFacade.getAllDatasources(listener);

        // Verify
        ArgumentCaptor<List<Datasource>> captor = ArgumentCaptor.forClass(List.class);
        verify(listener).onResponse(captor.capture());
        assertEquals(datasources, captor.getValue());
    }

    private SearchHits getMockedSearchHits(List<Datasource> datasources) {
        SearchHit[] searchHitArray = datasources.stream().map(this::toBytesReference).map(this::toSearchHit).toArray(SearchHit[]::new);

        return new SearchHits(searchHitArray, new TotalHits(1l, TotalHits.Relation.EQUAL_TO), 1);
    }

    private GetResponse getMockedGetResponse(Datasource datasource) {
        GetResponse response = mock(GetResponse.class);
        when(response.isExists()).thenReturn(datasource != null);
        when(response.getSourceAsBytesRef()).thenReturn(toBytesReference(datasource));
        return response;
    }

    private BytesReference toBytesReference(Datasource datasource) {
        if (datasource == null) {
            return null;
        }

        try {
            return BytesReference.bytes(datasource.toXContent(JsonXContent.contentBuilder(), null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private SearchHit toSearchHit(BytesReference bytesReference) {
        SearchHit searchHit = new SearchHit(Randomness.get().nextInt());
        searchHit.sourceRef(bytesReference);
        return searchHit;
    }
}
