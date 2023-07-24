/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;

import lombok.extern.log4j.Log4j2;

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.exceptions.ConcurrentModificationException;
import org.opensearch.geospatial.exceptions.ResourceInUseException;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.dao.DatasourceDao;
import org.opensearch.geospatial.ip2geo.dao.GeoIpDataDao;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoProcessorDao;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.ingest.IngestService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action to delete datasource
 */
@Log4j2
public class DeleteDatasourceTransportAction extends HandledTransportAction<DeleteDatasourceRequest, AcknowledgedResponse> {
    private static final long LOCK_DURATION_IN_SECONDS = 300l;
    private final Ip2GeoLockService lockService;
    private final IngestService ingestService;
    private final DatasourceDao datasourceDao;
    private final GeoIpDataDao geoIpDataDao;
    private final Ip2GeoProcessorDao ip2GeoProcessorDao;
    private final ThreadPool threadPool;

    /**
     * Constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param lockService the lock service
     * @param ingestService the ingest service
     * @param datasourceDao the datasource facade
     */
    @Inject
    public DeleteDatasourceTransportAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Ip2GeoLockService lockService,
        final IngestService ingestService,
        final DatasourceDao datasourceDao,
        final GeoIpDataDao geoIpDataDao,
        final Ip2GeoProcessorDao ip2GeoProcessorDao,
        final ThreadPool threadPool
    ) {
        super(DeleteDatasourceAction.NAME, transportService, actionFilters, DeleteDatasourceRequest::new);
        this.lockService = lockService;
        this.ingestService = ingestService;
        this.datasourceDao = datasourceDao;
        this.geoIpDataDao = geoIpDataDao;
        this.ip2GeoProcessorDao = ip2GeoProcessorDao;
        this.threadPool = threadPool;
    }

    /**
     * We delete datasource regardless of its state as long as we can acquire a lock
     *
     * @param task the task
     * @param request the request
     * @param listener the listener
     */
    @Override
    protected void doExecute(final Task task, final DeleteDatasourceRequest request, final ActionListener<AcknowledgedResponse> listener) {
        lockService.acquireLock(request.getName(), LOCK_DURATION_IN_SECONDS, ActionListener.wrap(lock -> {
            if (lock == null) {
                listener.onFailure(
                    new ConcurrentModificationException("another processor is holding a lock on the resource. Try again later")
                );
                return;
            }
            try {
                // TODO: makes every sub-methods as async call to avoid using a thread in generic pool
                threadPool.generic().submit(() -> {
                    try {
                        deleteDatasource(request.getName());
                        lockService.releaseLock(lock);
                        listener.onResponse(new AcknowledgedResponse(true));
                    } catch (Exception e) {
                        lockService.releaseLock(lock);
                        listener.onFailure(e);
                    }
                });
            } catch (Exception e) {
                lockService.releaseLock(lock);
                listener.onFailure(e);
            }
        }, exception -> { listener.onFailure(exception); }));
    }

    @VisibleForTesting
    protected void deleteDatasource(final String datasourceName) throws IOException {
        Datasource datasource = datasourceDao.getDatasource(datasourceName);
        if (datasource == null) {
            throw new ResourceNotFoundException("no such datasource exist");
        }

        setDatasourceStateAsDeleting(datasource);
        geoIpDataDao.deleteIp2GeoDataIndex(datasource.getIndices());
        datasourceDao.deleteDatasource(datasource);
    }

    private void setDatasourceStateAsDeleting(final Datasource datasource) {
        if (ip2GeoProcessorDao.getProcessors(datasource.getName()).isEmpty() == false) {
            throw new ResourceInUseException("datasource is being used by one of processors");
        }

        DatasourceState previousState = datasource.getState();
        datasource.setState(DatasourceState.DELETING);
        datasourceDao.updateDatasource(datasource);

        // Check again as processor might just have been created.
        // If it fails to update the state back to the previous state, the new processor
        // will fail to convert an ip to a geo data.
        // In such case, user have to delete the processor and delete this datasource again.
        if (ip2GeoProcessorDao.getProcessors(datasource.getName()).isEmpty() == false) {
            datasource.setState(previousState);
            datasourceDao.updateDatasource(datasource);
            throw new ResourceInUseException("datasource is being used by one of processors");
        }
    }
}
