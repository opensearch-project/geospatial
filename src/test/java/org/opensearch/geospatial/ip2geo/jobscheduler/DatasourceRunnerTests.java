/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

import lombok.SneakyThrows;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

public class DatasourceRunnerTests extends Ip2GeoTestCase {
    @Before
    public void init() {
        DatasourceRunner.getJobRunnerInstance()
            .initialize(clusterService, datasourceUpdateService, ip2GeoExecutor, datasourceDao, ip2GeoLockService);
    }

    public void testRunJob_whenInvalidClass_thenThrowException() {
        JobDocVersion jobDocVersion = new JobDocVersion(randomInt(), randomInt(), randomInt());
        String jobIndexName = randomLowerCaseString();
        String jobId = randomLowerCaseString();
        JobExecutionContext jobExecutionContext = new JobExecutionContext(Instant.now(), jobDocVersion, lockService, jobIndexName, jobId);
        ScheduledJobParameter jobParameter = mock(ScheduledJobParameter.class);

        // Run
        expectThrows(IllegalStateException.class, () -> DatasourceRunner.getJobRunnerInstance().runJob(jobParameter, jobExecutionContext));
    }

    @SneakyThrows
    public void testRunJob_whenValidInput_thenSucceed() {
        JobDocVersion jobDocVersion = new JobDocVersion(randomInt(), randomInt(), randomInt());
        String jobIndexName = randomLowerCaseString();
        String jobId = randomLowerCaseString();
        JobExecutionContext jobExecutionContext = new JobExecutionContext(Instant.now(), jobDocVersion, lockService, jobIndexName, jobId);
        Datasource datasource = randomDatasource();

        LockModel lockModel = randomLockModel();
        when(ip2GeoLockService.acquireLock(datasource.getName(), Ip2GeoLockService.LOCK_DURATION_IN_SECONDS)).thenReturn(
            Optional.of(lockModel)
        );

        // Run
        DatasourceRunner.getJobRunnerInstance().runJob(datasource, jobExecutionContext);

        // Verify
        verify(ip2GeoLockService).acquireLock(datasource.getName(), Ip2GeoLockService.LOCK_DURATION_IN_SECONDS);
        verify(datasourceDao).getDatasource(datasource.getName());
        verify(ip2GeoLockService).releaseLock(lockModel);
    }

    @SneakyThrows
    public void testUpdateDatasourceRunner_whenExceptionBeforeAcquiringLock_thenNoReleaseLock() {
        ScheduledJobParameter jobParameter = mock(ScheduledJobParameter.class);
        when(jobParameter.getName()).thenReturn(GeospatialTestHelper.randomLowerCaseString());
        when(ip2GeoLockService.acquireLock(jobParameter.getName(), Ip2GeoLockService.LOCK_DURATION_IN_SECONDS)).thenThrow(
            new RuntimeException()
        );

        // Run
        expectThrows(Exception.class, () -> DatasourceRunner.getJobRunnerInstance().updateDatasourceRunner(jobParameter).run());

        // Verify
        verify(ip2GeoLockService, never()).releaseLock(any());
    }

    @SneakyThrows
    public void testUpdateDatasourceRunner_whenExceptionAfterAcquiringLock_thenReleaseLock() {
        ScheduledJobParameter jobParameter = mock(ScheduledJobParameter.class);
        when(jobParameter.getName()).thenReturn(GeospatialTestHelper.randomLowerCaseString());
        LockModel lockModel = randomLockModel();
        when(ip2GeoLockService.acquireLock(jobParameter.getName(), Ip2GeoLockService.LOCK_DURATION_IN_SECONDS)).thenReturn(
            Optional.of(lockModel)
        );
        when(datasourceDao.getDatasource(jobParameter.getName())).thenThrow(new RuntimeException());

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasourceRunner(jobParameter).run();

        // Verify
        verify(ip2GeoLockService).releaseLock(any());
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
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, mock(Runnable.class));

        // Verify
        assertFalse(datasource.isEnabled());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceDao).updateDatasource(datasource);
    }

    @SneakyThrows
    public void testUpdateDatasource_whenValidInput_thenSucceed() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        Runnable renewLock = mock(Runnable.class);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, renewLock);

        // Verify
        verify(datasourceUpdateService, times(2)).deleteUnusedIndices(datasource);
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource, renewLock);
        verify(datasourceUpdateService).updateDatasource(datasource, datasource.getUserSchedule(), DatasourceTask.ALL);
    }

    @SneakyThrows
    public void testUpdateDatasource_whenDeleteTask_thenDeleteOnly() {
        Datasource datasource = randomDatasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.setTask(DatasourceTask.DELETE_UNUSED_INDICES);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        Runnable renewLock = mock(Runnable.class);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, renewLock);

        // Verify
        verify(datasourceUpdateService, times(2)).deleteUnusedIndices(datasource);
        verify(datasourceUpdateService, never()).updateOrCreateGeoIpData(datasource, renewLock);
        verify(datasourceUpdateService).updateDatasource(datasource, datasource.getUserSchedule(), DatasourceTask.ALL);
    }

    @SneakyThrows
    public void testUpdateDatasource_whenExpired_thenDeleteIndicesAgain() {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSkippedAt(null);
        datasource.getUpdateStats()
            .setLastSucceededAt(Instant.now().minus(datasource.getDatabase().getValidForInDays() + 1, ChronoUnit.DAYS));
        datasource.setState(DatasourceState.AVAILABLE);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        Runnable renewLock = mock(Runnable.class);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, renewLock);

        // Verify
        verify(datasourceUpdateService, times(3)).deleteUnusedIndices(datasource);
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource, renewLock);
        verify(datasourceUpdateService).updateDatasource(datasource, datasource.getUserSchedule(), DatasourceTask.ALL);
    }

    @SneakyThrows
    public void testUpdateDatasource_whenWillExpire_thenScheduleDeleteTask() {
        Datasource datasource = randomDatasource();
        datasource.getUpdateStats().setLastSkippedAt(null);
        datasource.getUpdateStats()
            .setLastSucceededAt(Instant.now().minus(datasource.getDatabase().getValidForInDays(), ChronoUnit.DAYS).plusSeconds(60));
        datasource.setState(DatasourceState.AVAILABLE);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        Runnable renewLock = mock(Runnable.class);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, renewLock);

        // Verify
        verify(datasourceUpdateService, times(2)).deleteUnusedIndices(datasource);
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource, renewLock);

        ArgumentCaptor<IntervalSchedule> captor = ArgumentCaptor.forClass(IntervalSchedule.class);
        verify(datasourceUpdateService).updateDatasource(eq(datasource), captor.capture(), eq(DatasourceTask.DELETE_UNUSED_INDICES));
        assertTrue(Duration.between(datasource.expirationDay(), captor.getValue().getNextExecutionTime(Instant.now())).getSeconds() < 30);
    }

    @SneakyThrows
    public void testUpdateDatasourceExceptionHandling() {
        Datasource datasource = new Datasource();
        datasource.setName(randomLowerCaseString());
        datasource.getUpdateStats().setLastFailedAt(null);
        when(datasourceDao.getDatasource(datasource.getName())).thenReturn(datasource);
        doThrow(new RuntimeException("test failure")).when(datasourceUpdateService).deleteUnusedIndices(any());

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource, mock(Runnable.class));

        // Verify
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceDao).updateDatasource(datasource);
    }
}
