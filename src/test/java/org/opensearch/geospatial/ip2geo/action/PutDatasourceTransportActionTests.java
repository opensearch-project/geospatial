/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.exceptions.ConcurrentModificationException;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.tasks.Task;

public class PutDatasourceTransportActionTests extends Ip2GeoTestCase {
    private PutDatasourceTransportAction action;

    @Before
    public void init() {
        action = new PutDatasourceTransportAction(
            transportService,
            actionFilters,
            threadPool,
            datasourceDao,
            datasourceUpdateService,
            ip2GeoLockService
        );
    }

    @SneakyThrows
    public void testDoExecute_whenFailedToAcquireLock_thenError() {
        validateDoExecute(null, null, null);
    }

    @SneakyThrows
    public void testDoExecute_whenAcquiredLock_thenSucceed() {
        validateDoExecute(randomLockModel(), null, null);
    }

    @SneakyThrows
    public void testDoExecute_whenExceptionBeforeAcquiringLock_thenError() {
        validateDoExecute(randomLockModel(), new RuntimeException(), null);
    }

    @SneakyThrows
    public void testDoExecute_whenExceptionAfterAcquiringLock_thenError() {
        validateDoExecute(randomLockModel(), null, new RuntimeException());
    }

    private void validateDoExecute(final LockModel lockModel, final Exception before, final Exception after) throws IOException {
        Task task = mock(Task.class);
        Datasource datasource = randomDatasource();
        PutDatasourceRequest request = new PutDatasourceRequest(datasource.getName());
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        if (after != null) {
            doThrow(after).when(datasourceDao).createIndexIfNotExists(any(StepListener.class));
        }

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        if (before == null) {
            // Run
            captor.getValue().onResponse(lockModel);

            // Verify
            if (lockModel == null) {
                verify(listener).onFailure(any(ConcurrentModificationException.class));
            }
            if (after != null) {
                verify(ip2GeoLockService).releaseLock(eq(lockModel));
                verify(listener).onFailure(after);
            } else {
                verify(ip2GeoLockService, never()).releaseLock(eq(lockModel));
            }
        } else {
            // Run
            captor.getValue().onFailure(before);
            // Verify
            verify(listener).onFailure(before);
        }
    }

    @SneakyThrows
    public void testInternalDoExecute_whenValidInput_thenSucceed() {
        PutDatasourceRequest request = new PutDatasourceRequest(GeospatialTestHelper.randomLowerCaseString());
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Run
        action.internalDoExecute(request, randomLockModel(), listener);

        // Verify
        ArgumentCaptor<StepListener> captor = ArgumentCaptor.forClass(StepListener.class);
        verify(datasourceDao).createIndexIfNotExists(captor.capture());

        // Run
        captor.getValue().onResponse(null);
        // Verify
        ArgumentCaptor<Datasource> datasourceCaptor = ArgumentCaptor.forClass(Datasource.class);
        ArgumentCaptor<ActionListener> actionListenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceDao).putDatasource(datasourceCaptor.capture(), actionListenerCaptor.capture());
        assertEquals(request.getName(), datasourceCaptor.getValue().getName());
        assertEquals(request.getEndpoint(), datasourceCaptor.getValue().getEndpoint());
        assertEquals(request.getUpdateInterval().days(), datasourceCaptor.getValue().getUserSchedule().getInterval());

        // Run next listener.onResponse
        actionListenerCaptor.getValue().onResponse(null);
        // Verify
        verify(listener).onResponse(new AcknowledgedResponse(true));
    }

    public void testGetIndexResponseListener_whenVersionConflict_thenFailure() {
        Datasource datasource = new Datasource();
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        action.getIndexResponseListener(datasource, randomLockModel(), listener)
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
        action.createDatasource(datasource, mock(Runnable.class));

        // Verify
        assertEquals(DatasourceState.CREATE_FAILED, datasource.getState());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceDao).updateDatasource(datasource);
        verify(datasourceUpdateService, never()).updateOrCreateGeoIpData(any(Datasource.class), any(Runnable.class));
    }

    @SneakyThrows
    public void testCreateDatasource_whenExceptionHappens_thenUpdateStateAsFailed() {
        Datasource datasource = new Datasource();
        doThrow(new RuntimeException()).when(datasourceUpdateService).updateOrCreateGeoIpData(any(Datasource.class), any(Runnable.class));

        // Run
        action.createDatasource(datasource, mock(Runnable.class));

        // Verify
        assertEquals(DatasourceState.CREATE_FAILED, datasource.getState());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceDao).updateDatasource(datasource);
    }

    @SneakyThrows
    public void testCreateDatasource_whenValidInput_thenUpdateStateAsCreating() {
        Datasource datasource = new Datasource();

        Runnable renewLock = mock(Runnable.class);
        // Run
        action.createDatasource(datasource, renewLock);

        // Verify
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource, renewLock);
        assertEquals(DatasourceState.CREATING, datasource.getState());
    }
}
