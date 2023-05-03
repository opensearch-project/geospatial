/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.time.Instant;

import org.junit.Before;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;

public class DatasourceRunnerTests extends Ip2GeoTestCase {
    @Before
    public void init() {
        DatasourceRunner.getJobRunnerInstance()
            .initialize(clusterService, client, datasourceUpdateService, ip2GeoExecutor, datasourceFacade);
    }

    public void testRunJobInvalidClass() {
        JobExecutionContext jobExecutionContext = mock(JobExecutionContext.class);
        ScheduledJobParameter jobParameter = mock(ScheduledJobParameter.class);
        expectThrows(IllegalStateException.class, () -> DatasourceRunner.getJobRunnerInstance().runJob(jobParameter, jobExecutionContext));
    }

    public void testRunJob() {
        JobDocVersion jobDocVersion = new JobDocVersion(randomInt(), randomInt(), randomInt());
        String jobIndexName = randomLowerCaseString();
        String jobId = randomLowerCaseString();
        JobExecutionContext jobExecutionContext = new JobExecutionContext(Instant.now(), jobDocVersion, lockService, jobIndexName, jobId);
        Datasource datasource = new Datasource();

        // Run
        DatasourceRunner.getJobRunnerInstance().runJob(datasource, jobExecutionContext);

        // Verify
        verify(executorService).submit(any(Runnable.class));
    }

    public void testUpdateDatasourceNull() throws Exception {
        Datasource datasource = new Datasource();

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource);

        // Verify
        verify(datasourceUpdateService, never()).deleteUnusedIndices(any());
    }

    public void testUpdateDatasourceInvalidState() throws Exception {
        Datasource datasource = new Datasource();
        datasource.enable();
        datasource.getUpdateStats().setLastFailedAt(null);
        datasource.setState(randomStateExcept(DatasourceState.AVAILABLE));
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource);

        // Verify
        assertFalse(datasource.isEnabled());
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }

    public void testUpdateDatasource() throws Exception {
        Datasource datasource = new Datasource();
        datasource.setState(DatasourceState.AVAILABLE);
        datasource.setName(randomLowerCaseString());
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource);

        // Verify
        verify(datasourceUpdateService, times(2)).deleteUnusedIndices(datasource);
        verify(datasourceUpdateService).updateOrCreateGeoIpData(datasource);
    }

    public void testUpdateDatasourceExceptionHandling() throws Exception {
        Datasource datasource = new Datasource();
        datasource.setName(randomLowerCaseString());
        datasource.getUpdateStats().setLastFailedAt(null);
        when(datasourceFacade.getDatasource(datasource.getName())).thenReturn(datasource);
        doThrow(new RuntimeException("test failure")).when(datasourceUpdateService).deleteUnusedIndices(any());

        // Run
        DatasourceRunner.getJobRunnerInstance().updateDatasource(datasource);

        // Verify
        assertNotNull(datasource.getUpdateStats().getLastFailedAt());
        verify(datasourceFacade).updateDatasource(datasource);
    }
}
