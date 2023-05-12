/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;

import lombok.extern.log4j.Log4j2;

import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoProcessorFacade;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.ingest.IngestService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to delete datasource
 */
@Log4j2
public class DeleteDatasourceTransportAction extends HandledTransportAction<DeleteDatasourceRequest, AcknowledgedResponse> {
    private static final long LOCK_DURATION_IN_SECONDS = 300l;
    private final Ip2GeoLockService lockService;
    private final IngestService ingestService;
    private final DatasourceFacade datasourceFacade;
    private final Ip2GeoProcessorFacade ip2GeoProcessorFacade;

    /**
     * Constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param lockService the lock service
     * @param ingestService the ingest service
     * @param datasourceFacade the datasource facade
     */
    @Inject
    public DeleteDatasourceTransportAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Ip2GeoLockService lockService,
        final IngestService ingestService,
        final DatasourceFacade datasourceFacade,
        final Ip2GeoProcessorFacade ip2GeoProcessorFacade
    ) {
        super(DeleteDatasourceAction.NAME, transportService, actionFilters, DeleteDatasourceRequest::new);
        this.lockService = lockService;
        this.ingestService = ingestService;
        this.datasourceFacade = datasourceFacade;
        this.ip2GeoProcessorFacade = ip2GeoProcessorFacade;
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
                listener.onFailure(new OpenSearchException("another processor is holding a lock on the resource. Try again later"));
                return;
            }
            try {
                deleteDatasource(request.getName());
                listener.onResponse(new AcknowledgedResponse(true));
            } catch (Exception e) {
                listener.onFailure(e);
            } finally {
                lockService.releaseLock(
                    lock,
                    ActionListener.wrap(
                        released -> { log.info("Released lock for datasource[{}]", request.getName()); },
                        exception -> { log.error("Failed to release the lock", exception); }
                    )
                );
            }
        }, exception -> { listener.onFailure(exception); }));
    }

    @VisibleForTesting
    protected void deleteDatasource(final String datasourceName) throws IOException {
        Datasource datasource = datasourceFacade.getDatasource(datasourceName);
        if (datasource == null) {
            throw new ResourceNotFoundException("no such datasource exist");
        }

        setDatasourceStateAsDeleting(datasource);
        datasourceFacade.deleteDatasource(datasource);
    }

    private void setDatasourceStateAsDeleting(final Datasource datasource) {
        if (ip2GeoProcessorFacade.getProcessors(datasource.getName()).isEmpty() == false) {
            throw new OpenSearchException("datasource is being used by one of processors");
        }

        DatasourceState previousState = datasource.getState();
        datasource.setState(DatasourceState.DELETING);
        datasourceFacade.updateDatasource(datasource);

        // Check again as processor might just have been created.
        // If it fails to update the state back to the previous state, the new processor
        // will fail to convert an ip to a geo data.
        // In such case, user have to delete the processor and delete this datasource again.
        if (ip2GeoProcessorFacade.getProcessors(datasource.getName()).isEmpty() == false) {
            datasource.setState(previousState);
            datasourceFacade.updateDatasource(datasource);
            throw new OpenSearchException("datasource is being used by one of processors");
        }
    }
}
