/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.log4j.Log4j2;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutor;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

/**
 * Datasource update task
 *
 * This is a background task which is responsible for updating GeoIp data
 */
@Log4j2
public class DatasourceRunner implements ScheduledJobRunner {
    private static final int DELETE_INDEX_RETRY_IN_MIN = 15;
    private static final int DELETE_INDEX_DELAY_IN_MILLIS = 10000;

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
    private Ip2GeoLockService ip2GeoLockService;
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
        final DatasourceFacade datasourceFacade,
        final Ip2GeoLockService ip2GeoLockService
    ) {
        this.clusterService = clusterService;
        this.datasourceUpdateService = datasourceUpdateService;
        this.ip2GeoExecutor = ip2GeoExecutor;
        this.datasourceFacade = datasourceFacade;
        this.ip2GeoLockService = ip2GeoLockService;
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

        ip2GeoExecutor.forDatasourceUpdate().submit(updateDatasourceRunner(jobParameter));
    }

    /**
     * Update GeoIP data
     *
     * Lock is used so that only one of nodes run this task.
     *
     * @param jobParameter job parameter
     */
    @VisibleForTesting
    protected Runnable updateDatasourceRunner(final ScheduledJobParameter jobParameter) {
        return () -> {
            Optional<LockModel> lockModel = ip2GeoLockService.acquireLock(
                jobParameter.getName(),
                Ip2GeoLockService.LOCK_DURATION_IN_SECONDS
            );
            if (lockModel.isEmpty()) {
                log.error("Failed to update. Another processor is holding a lock for datasource[{}]", jobParameter.getName());
                return;
            }

            LockModel lock = lockModel.get();
            try {
                updateDatasource(jobParameter, ip2GeoLockService.getRenewLockRunnable(new AtomicReference<>(lock)));
            } catch (Exception e) {
                log.error("Failed to update datasource[{}]", jobParameter.getName(), e);
            } finally {
                ip2GeoLockService.releaseLock(lock);
            }
        };
    }

    @VisibleForTesting
    protected void updateDatasource(final ScheduledJobParameter jobParameter, final Runnable renewLock) throws IOException {
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
            if (DatasourceTask.DELETE_UNUSED_INDICES.equals(datasource.getTask()) == false) {
                datasourceUpdateService.updateOrCreateGeoIpData(datasource, renewLock);
            }
            datasourceUpdateService.deleteUnusedIndices(datasource);
        } catch (Exception e) {
            log.error("Failed to update datasource for {}", datasource.getName(), e);
            datasource.getUpdateStats().setLastFailedAt(Instant.now());
            datasourceFacade.updateDatasource(datasource);
        } finally {
            postProcessing(datasource);
        }
    }

    private void postProcessing(final Datasource datasource) {
        if (datasource.isExpired()) {
            // Try to delete again as it could have just been expired
            datasourceUpdateService.deleteUnusedIndices(datasource);
            datasourceUpdateService.updateDatasource(datasource, datasource.getUserSchedule(), DatasourceTask.ALL);
            return;
        }

        if (datasource.willExpire(datasource.getUserSchedule().getNextExecutionTime(Instant.now()))) {
            IntervalSchedule intervalSchedule = new IntervalSchedule(
                datasource.expirationDay(),
                DELETE_INDEX_RETRY_IN_MIN,
                ChronoUnit.MINUTES,
                DELETE_INDEX_DELAY_IN_MILLIS
            );
            datasourceUpdateService.updateDatasource(datasource, intervalSchedule, DatasourceTask.DELETE_UNUSED_INDICES);
        } else {
            datasourceUpdateService.updateDatasource(datasource, datasource.getUserSchedule(), DatasourceTask.ALL);
        }
    }
}
