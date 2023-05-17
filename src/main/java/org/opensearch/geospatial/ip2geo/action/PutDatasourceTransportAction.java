/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService.LOCK_DURATION_IN_SECONDS;

import java.time.Instant;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.log4j.Log4j2;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.exceptions.ConcurrentModificationException;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutor;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceUpdateService;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action to create datasource
 */
@Log4j2
public class PutDatasourceTransportAction extends HandledTransportAction<PutDatasourceRequest, AcknowledgedResponse> {
    private final ThreadPool threadPool;
    private final DatasourceFacade datasourceFacade;
    private final DatasourceUpdateService datasourceUpdateService;
    private final Ip2GeoLockService lockService;
    private final Ip2GeoExecutor ip2GeoExecutor;

    /**
     * Default constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param threadPool the thread pool
     * @param datasourceFacade the datasource facade
     * @param datasourceUpdateService the datasource update service
     * @param lockService the lock service
     * @param ip2GeoExecutor the ip2Geo executor
     */
    @Inject
    public PutDatasourceTransportAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final ThreadPool threadPool,
        final DatasourceFacade datasourceFacade,
        final DatasourceUpdateService datasourceUpdateService,
        final Ip2GeoLockService lockService,
        final Ip2GeoExecutor ip2GeoExecutor
    ) {
        super(PutDatasourceAction.NAME, transportService, actionFilters, PutDatasourceRequest::new);
        this.threadPool = threadPool;
        this.datasourceFacade = datasourceFacade;
        this.datasourceUpdateService = datasourceUpdateService;
        this.lockService = lockService;
        this.ip2GeoExecutor = ip2GeoExecutor;
    }

    @Override
    protected void doExecute(final Task task, final PutDatasourceRequest request, final ActionListener<AcknowledgedResponse> listener) {
        lockService.acquireLock(request.getName(), LOCK_DURATION_IN_SECONDS, ActionListener.wrap(lock -> {
            if (lock == null) {
                listener.onFailure(
                    new ConcurrentModificationException("another processor is holding a lock on the resource. Try again later")
                );
                return;
            }
            try {
                internalDoExecute(request, lock, listener);
            } catch (Exception e) {
                lockService.releaseLock(lock);
                listener.onFailure(e);
            }
        }, exception -> { listener.onFailure(exception); }));
    }

    /**
     * This method takes lock as a parameter and is responsible for releasing lock
     * unless exception is thrown
     */
    @VisibleForTesting
    protected void internalDoExecute(
        final PutDatasourceRequest request,
        final LockModel lock,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        StepListener<Void> createIndexStep = new StepListener<>();
        datasourceFacade.createIndexIfNotExists(createIndexStep);
        createIndexStep.whenComplete(v -> {
            Datasource datasource = Datasource.Builder.build(request);
            datasourceFacade.putDatasource(datasource, getIndexResponseListener(datasource, lock, listener));
        }, exception -> {
            lockService.releaseLock(lock);
            listener.onFailure(exception);
        });
    }

    /**
     * This method takes lock as a parameter and is responsible for releasing lock
     * unless exception is thrown
     */
    @VisibleForTesting
    protected ActionListener<IndexResponse> getIndexResponseListener(
        final Datasource datasource,
        final LockModel lock,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(final IndexResponse indexResponse) {
                try {
                    ip2GeoExecutor.forDatasourceCreate().submit(() -> {
                        AtomicReference<LockModel> lockReference = new AtomicReference<>(lock);
                        try {
                            createDatasource(datasource, lockService.getRenewLockRunnable(lockReference));
                        } finally {
                            lockService.releaseLock(lockReference.get());
                        }
                    });
                } catch (RejectedExecutionException e) {
                    lockService.releaseLock(lock);
                    datasourceFacade.deleteDatasource(datasource);
                    listener.onFailure(
                        new OpenSearchRejectedExecutionException("another datasource creation is in progress in current node")
                    );
                    return;
                }
                listener.onResponse(new AcknowledgedResponse(true));
            }

            @Override
            public void onFailure(final Exception e) {
                lockService.releaseLock(lock);
                if (e instanceof VersionConflictEngineException) {
                    listener.onFailure(new ResourceAlreadyExistsException("datasource [{}] already exists", datasource.getName()));
                } else {
                    listener.onFailure(e);
                }
            }
        };
    }

    @VisibleForTesting
    protected void createDatasource(final Datasource datasource, final Runnable renewLock) {
        if (DatasourceState.CREATING.equals(datasource.getState()) == false) {
            log.error("Invalid datasource state. Expecting {} but received {}", DatasourceState.CREATING, datasource.getState());
            markDatasourceAsCreateFailed(datasource);
            return;
        }

        try {
            datasourceUpdateService.updateOrCreateGeoIpData(datasource, renewLock);
        } catch (Exception e) {
            log.error("Failed to create datasource for {}", datasource.getName(), e);
            markDatasourceAsCreateFailed(datasource);
        }
    }

    private void markDatasourceAsCreateFailed(final Datasource datasource) {
        datasource.getUpdateStats().setLastFailedAt(Instant.now());
        datasource.setState(DatasourceState.CREATE_FAILED);
        try {
            datasourceFacade.updateDatasource(datasource);
        } catch (Exception e) {
            log.error("Failed to mark datasource state as CREATE_FAILED for {}", datasource.getName(), e);
        }
    }
}
