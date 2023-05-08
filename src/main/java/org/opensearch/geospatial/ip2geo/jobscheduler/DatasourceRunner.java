/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import java.io.IOException;
import java.time.Instant;

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutor;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;

/**
 * Datasource update task
 *
 * This is a background task which is responsible for updating GeoIp data
 */
@Log4j2
public class DatasourceRunner implements ScheduledJobRunner {

    private static DatasourceRunner INSTANCE;

    /**
     * Return a singleton job runner instance
     * @return job runner
     */
    public static DatasourceRunner getJobRunnerInstance() {
        if (INSTANCE != null) {
            return INSTANCE;
        }
        synchronized (DatasourceRunner.class) {
            if (INSTANCE != null) {
                return INSTANCE;
            }
            INSTANCE = new DatasourceRunner();
            return INSTANCE;
        }
    }

    private ClusterService clusterService;
    private DatasourceUpdateService datasourceUpdateService;
    private Ip2GeoExecutor ip2GeoExecutor;
    private DatasourceFacade datasourceFacade;
    private boolean initialized;

    private DatasourceRunner() {
        // Singleton class, use getJobRunner method instead of constructor
    }

    /**
     * Initialize timeout and indexingBulkSize from settings
     */
    public void initialize(
        final ClusterService clusterService,
        final DatasourceUpdateService datasourceUpdateService,
        final Ip2GeoExecutor ip2GeoExecutor,
        final DatasourceFacade datasourceFacade
    ) {
        this.clusterService = clusterService;
        this.datasourceUpdateService = datasourceUpdateService;
        this.ip2GeoExecutor = ip2GeoExecutor;
        this.datasourceFacade = datasourceFacade;
        this.initialized = true;
    }

    @Override
    public void runJob(final ScheduledJobParameter jobParameter, final JobExecutionContext context) {
        if (initialized == false) {
            throw new AssertionError("this instance is not initialized");
        }

        log.info("Update job started for a datasource[{}]", jobParameter.getName());
        if (jobParameter instanceof Datasource == false) {
            throw new IllegalStateException(
                "job parameter is not instance of Datasource, type: " + jobParameter.getClass().getCanonicalName()
            );
        }

        ip2GeoExecutor.forDatasourceUpdate().submit(updateDatasourceRunner(jobParameter, context));
    }

    /**
     * Update GeoIP data
     *
     * Lock is used so that only one of nodes run this task.
     * Lock duration is 1 hour to avoid refreshing. This is okay because update interval is 1 day minimum.
     *
     * @param jobParameter job parameter
     * @param context context
     */
    @VisibleForTesting
    protected Runnable updateDatasourceRunner(final ScheduledJobParameter jobParameter, final JobExecutionContext context) {
        final LockService lockService = context.getLockService();
        return () -> {
            lockService.acquireLock(jobParameter, context, ActionListener.wrap(lock -> {
                if (lock == null) {
                    return;
                }
                try {
                    updateDatasource(jobParameter);
                } finally {
                    lockService.release(
                        lock,
                        ActionListener.wrap(released -> {}, exception -> { log.error("Failed to release lock [{}]", lock, exception); })
                    );
                }
            }, exception -> { log.error("Failed to acquire lock for job [{}]", jobParameter.getName(), exception); }));
        };
    }

    @VisibleForTesting
    protected void updateDatasource(final ScheduledJobParameter jobParameter) throws IOException {
        Datasource datasource = datasourceFacade.getDatasource(jobParameter.getName());
        /**
         * If delete request comes while update task is waiting on a queue for other update tasks to complete,
         * because update task for this datasource didn't acquire a lock yet, delete request is processed.
         * When it is this datasource's turn to run, it will find that the datasource is deleted already.
         * Therefore, we stop the update process when data source does not exist.
         */
        if (datasource == null) {
            log.info("Datasource[{}] does not exist", jobParameter.getName());
            return;
        }

        if (DatasourceState.AVAILABLE.equals(datasource.getState()) == false) {
            log.error("Invalid datasource state. Expecting {} but received {}", DatasourceState.AVAILABLE, datasource.getState());
            datasource.disable();
            datasource.getUpdateStats().setLastFailedAt(Instant.now());
            datasourceFacade.updateDatasource(datasource);
            return;
        }

        try {
            datasourceUpdateService.deleteUnusedIndices(datasource);
            datasourceUpdateService.updateOrCreateGeoIpData(datasource);
            datasourceUpdateService.deleteUnusedIndices(datasource);
        } catch (Exception e) {
            log.error("Failed to update datasource for {}", datasource.getName(), e);
            datasource.getUpdateStats().setLastFailedAt(Instant.now());
            datasourceFacade.updateDatasource(datasource);
        }
    }
}
