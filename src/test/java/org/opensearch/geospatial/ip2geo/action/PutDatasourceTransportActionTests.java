/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Before;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.tasks.Task;

public class PutDatasourceTransportActionTests extends Ip2GeoTestCase {
    private PutDatasourceTransportAction action;

    @Before
    public void init() {
        action = new PutDatasourceTransportAction(
            transportService,
            actionFilters,
            verifyingClient,
            threadPool,
            datasourceFacade,
            datasourceUpdateService
        );
    }

    public void testDoExecute() throws Exception {
        Task task = mock(Task.class);
        PutDatasourceRequest request = new PutDatasourceRequest("test");
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof IndexRequest);
            IndexRequest indexRequest = (IndexRequest) actionRequest;
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, indexRequest.index());
            assertEquals(request.getDatasourceName(), indexRequest.id());
            assertEquals(WriteRequest.RefreshPolicy.IMMEDIATE, indexRequest.getRefreshPolicy());
            assertEquals(DocWriteRequest.OpType.CREATE, indexRequest.opType());
            return null;
        });
        action.doExecute(task, request, listener);
        verify(verifyingClient).index(any(IndexRequest.class), any(ActionListener.class));
        verify(listener).onResponse(new AcknowledgedResponse(true));
    }

    public void testIndexResponseListenerFailure() {
        Datasource datasource = new Datasource();
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        action.getIndexResponseListener(datasource, listener)
            .onFailure(
                new VersionConflictEngineException(
                    null,
                    GeospatialTestHelper.randomLowerCaseString(),
                    GeospatialTestHelper.randomLowerCaseString()
                )
            );
        verify(listener).onFailure(any(ResourceAlreadyExistsException.class));
    }

    public void testCreateDatasourceInvalidState() throws Exception {
        Datasource datasource = new Datasource();
        datasource.setState(randomStateExcept(DatasourceState.CREATING));
        datasource.getUpdateStats().setLastFailedAt(null);

        // Run
        action.createDatasource(datasource);

        // Verify
        assertEquals(DatasourceState.CREATE_FAILED, datasource.getState());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }

    public void testCreateDatasourceWithException() throws Exception {
        Datasource datasource = new Datasource();
        doThrow(new RuntimeException()).when(datasourceUpdateService).updateOrCreateGeoIpData(datasource);

        // Run
        action.createDatasource(datasource);

        // Verify
        assertEquals(DatasourceState.CREATE_FAILED, datasource.getState());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }

    public void testCreateDatasource() throws Exception {
        Datasource datasource = new Datasource();

        // Run
        action.createDatasource(datasource);

        // Verify
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource);
        assertEquals(DatasourceState.CREATING, datasource.getState());
    }
}
