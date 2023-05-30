/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.InvalidParameterException;
import java.util.List;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.exceptions.IncompatibleDatasourceException;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.tasks.Task;

public class UpdateDatasourceTransportActionTests extends Ip2GeoTestCase {
    private UpdateDatasourceTransportAction action;

    @Before
    public void init() {
        action = new UpdateDatasourceTransportAction(
            transportService,
            actionFilters,
            ip2GeoLockService,
            datasourceFacade,
            datasourceUpdateService
        );
    }

    public void testDoExecute_whenFailedToAcquireLock_thenError() {
        validateDoExecuteWithLockError(null);
    }

    public void testDoExecute_whenExceptionToAcquireLock_thenError() {
        validateDoExecuteWithLockError(new RuntimeException());
    }

    private void validateDoExecuteWithLockError(final Exception exception) {
        Task task = mock(Task.class);
        Datasource datasource = randomDatasource();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        if (exception == null) {
            // Run
            captor.getValue().onResponse(null);
            // Verify
            verify(listener).onFailure(any(OpenSearchException.class));
        } else {
            // Run
            captor.getValue().onFailure(exception);
            // Verify
            verify(listener).onFailure(exception);
        }
    }

    @SneakyThrows
    public void testDoExecute_whenValidInput_thenUpdate() {
        Datasource datasource = randomDatasource();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(sampleManifestUrl());
        // Sample manifest has validForDays of 30. Update interval should be less than that.
        request.setUpdateInterval(TimeValue.timeValueDays(Randomness.get().nextInt(29)));

        Task task = mock(Task.class);
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        when(datasourceUpdateService.getHeaderFields(request.getEndpoint())).thenReturn(datasource.getDatabase().getFields());
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        LockModel lockModel = randomLockModel();

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        // Run
        captor.getValue().onResponse(lockModel);

        // Verify
        verify(datasourceFacade).getDatasource(datasource.getName());
        verify(datasourceFacade).updateDatasource(datasource);
        verify(datasourceUpdateService).getHeaderFields(request.getEndpoint());
        assertEquals(request.getEndpoint(), datasource.getEndpoint());
        assertEquals(request.getUpdateInterval().days(), datasource.getUserSchedule().getInterval());
        verify(listener).onResponse(new AcknowledgedResponse(true));
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }

    @SneakyThrows
    public void testDoExecute_whenNoChangesInValues_thenNoUpdate() {
        Datasource datasource = randomDatasource();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setUpdateInterval(TimeValue.timeValueDays(datasource.getUserSchedule().getInterval()));
        request.setEndpoint(datasource.getEndpoint());

        Task task = mock(Task.class);
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        LockModel lockModel = randomLockModel();

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        // Run
        captor.getValue().onResponse(lockModel);

        // Verify
        verify(datasourceFacade).getDatasource(datasource.getName());
        verify(datasourceUpdateService, never()).getHeaderFields(anyString());
        verify(datasourceFacade, never()).updateDatasource(datasource);
        verify(listener).onResponse(new AcknowledgedResponse(true));
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }

    @SneakyThrows
    public void testDoExecute_whenNoDatasource_thenError() {
        Datasource datasource = randomDatasource();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());

        Task task = mock(Task.class);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        LockModel lockModel = randomLockModel();

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        // Run
        captor.getValue().onResponse(lockModel);

        // Verify
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals(ResourceNotFoundException.class, exceptionCaptor.getValue().getClass());
        exceptionCaptor.getValue().getMessage().contains("no such datasource exist");
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }

    @SneakyThrows
    public void testDoExecute_whenIncompatibleFields_thenError() {
        Datasource datasource = randomDatasource();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(sampleManifestUrl());

        Task task = mock(Task.class);
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        List<String> newFields = datasource.getDatabase().getFields().subList(0, 0);
        when(datasourceUpdateService.getHeaderFields(request.getEndpoint())).thenReturn(newFields);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        LockModel lockModel = randomLockModel();

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        // Run
        captor.getValue().onResponse(lockModel);

        // Verify
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals(IncompatibleDatasourceException.class, exceptionCaptor.getValue().getClass());
        exceptionCaptor.getValue().getMessage().contains("does not contain");
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }

    @SneakyThrows
    public void testDoExecute_whenInvalidUpdateInterval_thenError() {
        Datasource datasource = randomDatasource();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setUpdateInterval(TimeValue.timeValueDays(datasource.getDatabase().getValidForInDays()));

        Task task = mock(Task.class);
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);
        LockModel lockModel = randomLockModel();

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        // Run
        captor.getValue().onResponse(lockModel);

        // Verify
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener).onFailure(exceptionCaptor.capture());
        assertEquals(InvalidParameterException.class, exceptionCaptor.getValue().getClass());
        exceptionCaptor.getValue().getMessage().contains("should be smaller");
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }
}
