/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.io.IOException;
import java.time.Instant;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

public class DatasourceRunnerTests extends Ip2GeoTestCase {
    @Before
    public void init() {
        DatasourceRunner.getJobRunnerInstance()
            .initialize(clusterService, datasourceUpdateService, ip2GeoExecutor, datasourceFacade, ip2GeoLockService);
    }

    public void testRunJob_whenInvalidClass_thenThrowException() {
        JobExecutionContext jobExecutionContext = mock(JobExecutionContext.class);
        ScheduledJobParameter jobParameter = mock(ScheduledJobParameter.class);
        expectThrows(IllegalStateException.class, () -> DatasourceRunner.getJobRunnerInstance().runJob(jobParameter, jobExecutionContext));
    }

    public void testRunJob_whenValidInput_thenSucceed() {
        JobDocVersion jobDocVersion = new JobDocVersion(randomInt(), randomInt(), randomInt());
        String jobIndexName = randomLowerCaseString();
        String jobId = randomLowerCaseString();
        JobExecutionContext jobExecutionContext = new JobExecutionContext(Instant.now(), jobDocVersion, lockService, jobIndexName, jobId);
        Datasource datasource = randomDatasource();

        // Run
        DatasourceRunner.getJobRunnerInstance().runJob(datasource, jobExecutionContext);

        // Verify
        verify(ip2GeoLockService).acquireLock(
            eq(datasource.getName()),
            eq(Ip2GeoLockService.LOCK_DURATION_IN_SECONDS),
            any(ActionListener.class)
        );
    }

    @SneakyThrows
    public void testUpdateDatasourceRunner_whenFailedToAcquireLock_thenError() {
        validateDoExecute(null, null);
    }

    @SneakyThrows
    public void testUpdateDatasourceRunner_whenValidInput_thenSucceed() {
        String jobIndexName = GeospatialTestHelper.randomLowerCaseString();
        String jobId = GeospatialTestHelper.randomLowerCaseString();
        LockModel lockModel = new LockModel(jobIndexName, jobId, Instant.now(), randomPositiveLong(), false);
        validateDoExecute(lockModel, null);
    }

    @SneakyThrows
    public void testUpdateDatasourceRunner_whenException_thenError() {
        validateDoExecute(null, new RuntimeException());
    }

    private void validateDoExecute(final LockModel lockModel, final Exception exception) throws IOException {
        ScheduledJobParameter jobParameter = mock(ScheduledJobParameter.class);
        when(jobParameter.getName()).thenReturn(GeospatialTestHelper.randomLowerCaseString());

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasourceRunner(jobParameter).run();

        // Verify
        ArgumentCaptor<ActionListener<LockModel>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(ip2GeoLockService).acquireLock(eq(jobParameter.getName()), anyLong(), captor.capture());

        if (exception == null) {
            // Run
            captor.getValue().onResponse(lockModel);

            // Verify
            verify(ip2GeoLockService, lockModel == null ? never() : times(1)).releaseLock(eq(lockModel), any(ActionListener.class));
        } else {
            // Run
            captor.getValue().onFailure(exception);

            // Verify
            verify(ip2GeoLockService, never()).releaseLock(eq(lockModel), any(ActionListener.class));
        }
    }

    @SneakyThrows
    public void testUpdateDatasource_whenDatasourceDoesNotExist_thenDoNothing() {
        Datasource datasource = new Datasource();

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, mock(Runnable.class));

        // Verify
        verify(datasourceUpdateService, never()).deleteUnusedIndices(any());
    }

    @SneakyThrows
    public void testUpdateDatasource_whenInvalidState_thenUpdateLastFailedAt() {
        Datasource datasource = new Datasource();
        datasource.enable();
        datasource.getUpdateStats().setLastFailedAt(null);
        datasource.setState(randomStateExcept(DatasourceState.AVAILABLE));
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, mock(Runnable.class));

        // Verify
        assertFalse(datasource.isEnabled());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }

    @SneakyThrows
    public void testUpdateDatasource_whenValidInput_thenSucceed() {
        Datasource datasource = new Datasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.setName(randomLowerCaseString());
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        Runnable renewLock = mock(Runnable.class);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, renewLock);

        // Verify
        verify(datasourceUpdateService, times(2)).deleteUnusedIndices(datasource);
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource, renewLock);
    }

    @SneakyThrows
    public void testUpdateDatasourceExceptionHandling() {
        Datasource datasource = new Datasource();
        datasource.setName(randomLowerCaseString());
        datasource.getUpdateStats().setLastFailedAt(null);
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        doThrow(new RuntimeException("test failure")).when(datasourceUpdateService).deleteUnusedIndices(any());

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, mock(Runnable.class));

        // Verify
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }
}
