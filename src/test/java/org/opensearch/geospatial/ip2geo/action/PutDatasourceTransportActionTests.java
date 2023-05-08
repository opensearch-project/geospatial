/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.tasks.Task;

public class PutDatasourceTransportActionTests extends Ip2GeoTestCase {
    private PutDatasourceTransportAction action;

    @Before
    public void init() {
        action = new PutDatasourceTransportAction(transportService, actionFilters, threadPool, datasourceFacade, datasourceUpdateService);
    }

    @SneakyThrows
    public void testDoExecute_whenValidInput_thenSucceed() {
        Task task = mock(Task.class);
        PutDatasourceRequest request = new PutDatasourceRequest(GeospatialTestHelper.randomLowerCaseString());
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<StepListener> captor = ArgumentCaptor.forClass(StepListener.class);
        verify(datasourceFacade).createIndexIfNotExists(captor.capture());

        // Run
        captor.getValue().onResponse(null);
        // Verify
        ArgumentCaptor<Datasource> datasourceCaptor = ArgumentCaptor.forClass(Datasource.class);
        ArgumentCaptor<ActionListener> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceFacade).putDatasource(datasourceCaptor.capture(), actionListenerCaptor.capture());
        assertEquals(request.getName(), datasourceCaptor.getValue().getName());
        assertEquals(request.getEndpoint(), datasourceCaptor.getValue().getEndpoint());
        assertEquals(request.getUpdateInterval().days(), datasourceCaptor.getValue().getSchedule().getInterval());

        // Run next listener.onResponse
        actionListenerCaptor.getValue().onResponse(null);
        // Verify
        verify(listener).onResponse(new AcknowledgedResponse(true));
    }

    public void testGetIndexResponseListener_whenVersionConflict_thenFailure() {
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

    @SneakyThrows
    public void testCreateDatasource_whenInvalidState_thenUpdateStateAsFailed() {
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

    @SneakyThrows
    public void testCreateDatasource_whenExceptionHappens_thenUpdateStateAsFailed() {
        Datasource datasource = new Datasource();
        doThrow(new RuntimeException()).when(datasourceUpdateService).updateOrCreateGeoIpData(datasource);

        // Run
        action.createDatasource(datasource);

        // Verify
        assertEquals(DatasourceState.CREATE_FAILED, datasource.getState());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }

    @SneakyThrows
    public void testCreateDatasource_whenValidInput_thenUpdateStateAsCreating() {
        Datasource datasource = new Datasource();

        // Run
        action.createDatasource(datasource);

        // Verify
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource);
        assertEquals(DatasourceState.CREATING, datasource.getState());
    }
}
