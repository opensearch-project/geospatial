/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.tasks.Task;

public class DeleteDatasourceTransportActionTests extends Ip2GeoTestCase {
    private DeleteDatasourceTransportAction action;

    @Before
    public void init() {
        action = new DeleteDatasourceTransportAction(
            transportService,
            actionFilters,
            ip2GeoLockService,
            ingestService,
            datasourceDao,
            geoIpDataDao,
            ip2GeoProcessorDao,
            threadPool
        );
    }

    @SneakyThrows
    public void testDoExecute_whenFailedToAcquireLock_thenError() {
        validateDoExecute(null, null);
    }

    @SneakyThrows
    public void testDoExecute_whenValidInput_thenSucceed() {
        String jobIndexName = GeospatialTestHelper.randomLowerCaseString();
        String jobId = GeospatialTestHelper.randomLowerCaseString();
        LockModel lockModel = new LockModel(jobIndexName, jobId, Instant.now(), randomPositiveLong(), false);
        validateDoExecute(lockModel, null);
    }

    @SneakyThrows
    public void testDoExecute_whenException_thenError() {
        validateDoExecute(null, new RuntimeException());
    }

    private void validateDoExecute(final LockModel lockModel, final Exception exception) throws IOException {
        Task task = mock(Task.class);
        Datasource datasource = randomDatasource();
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        DeleteDatasourceRequest request = new DeleteDatasourceRequest(datasource.getName());
        ActionListener<AcknowledgedResponse> listener = mock(ActionListener.class);

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(datasource.getName()), anyLong(), captor.capture());

        if (exception == null) {
            // Run
            captor.getValue().onResponse(lockModel);

            // Verify
            if (lockModel == null) {
                verify(listener).onFailure(any(OpenSearchException.class));
            } else {
                verify(listener).onResponse(new AcknowledgedResponse(true));
                verify(ip2GeoLockService).releaseLock(eq(lockModel));
            }
        } else {
            // Run
            captor.getValue().onFailure(exception);
            // Verify
            verify(listener).onFailure(exception);
        }
    }

    @SneakyThrows
    public void testDeleteDatasource_whenNull_thenThrowException() {
        Datasource datasource = randomDatasource();
        expectThrows(ResourceNotFoundException.class, () -> action.deleteDatasource(datasource.getName()));
    }

    @SneakyThrows
    public void testDeleteDatasource_whenSafeToDelete_thenDelete() {
        Datasource datasource = randomDatasource();
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        when(ip2GeoProcessorDao.getProcessors(datasource.getName())).thenReturn(Collections.emptyList());

        // Run
        action.deleteDatasource(datasource.getName());

        // Verify
        assertEquals(DatasourceState.DELETING, datasource.getState());
        verify(datasourceDao).updateDatasource(datasource);
        InOrder inOrder = Mockito.inOrder(geoIpDataDao, datasourceDao);
        inOrder.verify(geoIpDataDao).deleteIp2GeoDataIndex(datasource.getIndices());
        inOrder.verify(datasourceDao).deleteDatasource(datasource);
    }

    @SneakyThrows
    public void testDeleteDatasource_whenProcessorIsUsingDatasource_thenThrowException() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        when(ip2GeoProcessorDao.getProcessors(datasource.getName())).thenReturn(Arrays.asList(randomIp2GeoProcessor(datasource.getName())));

        // Run
        expectThrows(OpenSearchException.class, () -> action.deleteDatasource(datasource.getName()));

        // Verify
        assertEquals(DatasourceState.AVAILABLE, datasource.getState());
        verify(datasourceDao, never()).updateDatasource(datasource);
        verify(geoIpDataDao, never()).deleteIp2GeoDataIndex(datasource.getIndices());
        verify(datasourceDao, never()).deleteDatasource(datasource);
    }

    @SneakyThrows
    public void testDeleteDatasource_whenProcessorIsCreatedDuringDeletion_thenThrowException() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        when(ip2GeoProcessorDao.getProcessors(datasource.getName())).thenReturn(
            Collections.emptyList(),
            Arrays.asList(randomIp2GeoProcessor(datasource.getName()))
        );

        // Run
        expectThrows(OpenSearchException.class, () -> action.deleteDatasource(datasource.getName()));

        // Verify
        verify(datasourceDao, times(2)).updateDatasource(datasource);
        verify(geoIpDataDao, never()).deleteIp2GeoDataIndex(datasource.getIndices());
        verify(datasourceDao, never()).deleteDatasource(datasource);
    }
}
