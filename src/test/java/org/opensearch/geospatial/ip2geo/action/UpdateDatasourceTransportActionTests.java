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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.exceptions.IncompatibleDatasourceException;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceTask;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.tasks.Task;

import lombok.SneakyThrows;

public class UpdateDatasourceTransportActionTests extends Ip2GeoTestCase {
    private UpdateDatasourceTransportAction action;

    @Before
    public void init() {
        action = new UpdateDatasourceTransportAction(
            transportService,
            actionFilters,
            ip2GeoLockService,
            datasourceDao,
            datasourceUpdateService,
            threadPool
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
        Datasource datasource = randomDatasource(Instant.now().minusSeconds(60));
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.setTask(DatasourceTask.DELETE_UNUSED_INDICES);
        Instant originalStartTime = datasource.getSchedule().getStartTime();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(anotherSampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(datasource.getSchedule().getInterval()));

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
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
        verify(datasourceDao).getDatasource(datasource.getName());
        verify(datasourceDao).updateDatasource(datasource);
        verify(datasourceUpdateService).getHeaderFields(request.getEndpoint());
        assertEquals(request.getEndpoint(), datasource.getEndpoint());
        assertEquals(request.getUpdateInterval().days(), datasource.getUserSchedule().getInterval());
        verify(listener).onResponse(new AcknowledgedResponse(true));
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
        assertTrue(originalStartTime.isBefore(datasource.getSchedule().getStartTime()));
        assertEquals(DatasourceTask.ALL, datasource.getTask());
    }

    @SneakyThrows
    public void testDoExecute_whenNoChangesInValues_thenNoUpdate() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(datasource.getEndpoint());

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
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
        verify(datasourceDao).getDatasource(datasource.getName());
        verify(datasourceUpdateService, never()).getHeaderFields(anyString());
        verify(datasourceDao, never()).updateDatasource(datasource);
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
    public void testDoExecute_whenNotInAvailableState_thenError() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.CREATE_FAILED);
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
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
        assertEquals(IllegalArgumentException.class, exceptionCaptor.getValue().getClass());
        exceptionCaptor.getValue().getMessage().contains("not in an available");
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }

    @SneakyThrows
    public void testDoExecute_whenIncompatibleFields_thenError() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(anotherSampleManifestUrl());

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
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
    public void testDoExecute_whenLargeUpdateInterval_thenError() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setUpdateInterval(TimeValue.timeValueDays(datasource.getDatabase().getValidForInDays()));

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
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

    @SneakyThrows
    public void testDoExecute_whenExpireWithNewUpdateInterval_thenError() {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSkippedAt(null);
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(datasource.getDatabase().getValidForInDays(), ChronoUnit.DAYS));
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
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
        assertEquals(IllegalArgumentException.class, exceptionCaptor.getValue().getClass());
        exceptionCaptor.getValue().getMessage().contains("will expire");
        verify(ip2GeoLockService).releaseLock(eq(lockModel));
    }

    @SneakyThrows
    public void testDoExecute_whenInvalidUrlInsideManifest_thenFail() {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSkippedAt(null);
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(datasource.getDatabase().getValidForInDays(), ChronoUnit.DAYS));
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(sampleManifestUrlWithInvalidUrl());

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Run
        Throwable throwable = expectThrows(IllegalArgumentException.class, () -> action.doExecute(task, request, listener));

        // Verify
        assertTrue(throwable.getMessage().contains("Invalid URL format is provided for url field in the manifest file"));
    }

    @SneakyThrows
    public void testDoExecute_whenInvalidManifestFile_thenFails() {
        String domain = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSkippedAt(null);
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(datasource.getDatabase().getValidForInDays(), ChronoUnit.DAYS));
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasource.getName());
        request.setEndpoint(String.format(Locale.ROOT, "https://%s.com", domain));

        Task task = mock(Task.class);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Run
        Throwable throwable = expectThrows(RuntimeException.class, () -> action.doExecute(task, request, listener));

        // Verify
        assertTrue(throwable.getMessage().contains("Error occurred while reading a file from"));
    }
}
