/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;
import java.time.Instant;

import lombok.extern.log4j.Log4j2;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.StepListener;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.geospatial.annotation.VisibleForTesting;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceUpdateService;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action to create datasource
 */
@Log4j2
public class PutDatasourceTransportAction extends HandledTransportAction<PutDatasourceRequest, AcknowledgedResponse> {
    private final Client client;
    private final ThreadPool threadPool;
    private final DatasourceFacade datasourceFacade;
    private final DatasourceUpdateService datasourceUpdateService;

    /**
     * Default constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param client the client
     * @param threadPool the thread pool
     */
    @Inject
    public PutDatasourceTransportAction(
        final TransportService transportService,
        final ActionFilters actionFilters,
        final Client client,
        final ThreadPool threadPool,
        final DatasourceFacade datasourceFacade,
        final DatasourceUpdateService datasourceUpdateService
    ) {
        super(PutDatasourceAction.NAME, transportService, actionFilters, PutDatasourceRequest::new);
        this.client = client;
        this.threadPool = threadPool;
        this.datasourceFacade = datasourceFacade;
        this.datasourceUpdateService = datasourceUpdateService;
    }

    @Override
    protected void doExecute(final Task task, final PutDatasourceRequest request, final ActionListener<AcknowledgedResponse> listener) {
        try {
            StepListener<Void> createIndexStep = new StepListener<>();
            datasourceFacade.createIndexIfNotExists(createIndexStep);
            createIndexStep.whenComplete(v -> putDatasource(request, listener), exception -> listener.onFailure(exception));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @VisibleForTesting
    protected void putDatasource(final PutDatasourceRequest request, final ActionListener<AcknowledgedResponse> listener)
        throws IOException {
        Datasource datasource = Datasource.Builder.build(request);
        IndexRequest indexRequest = new IndexRequest().index(DatasourceExtension.JOB_INDEX_NAME)
            .id(datasource.getName())
            .source(datasource.toXContent(JsonXContent.contentBuilder(), null))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .opType(DocWriteRequest.OpType.CREATE);
        client.index(indexRequest, getIndexResponseListener(datasource, listener));
    }

    @VisibleForTesting
    protected ActionListener<IndexResponse> getIndexResponseListener(
        final Datasource datasource,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        return new ActionListener<>() {
            @Override
            public void onResponse(final IndexResponse indexResponse) {
                // This is user initiated request. Therefore, we want to handle the first datasource update task in a generic thread
                // pool.
                threadPool.generic().submit(() -> { createDatasource(datasource); });
                listener.onResponse(new AcknowledgedResponse(true));
            }

            @Override
            public void onFailure(final Exception e) {
                if (e instanceof VersionConflictEngineException) {
                    listener.onFailure(new ResourceAlreadyExistsException("datasource [{}] already exists", datasource.getName()));
                } else {
                    listener.onFailure(e);
                }
            }
        };
    }

    @VisibleForTesting
    protected void createDatasource(final Datasource datasource) {
        if (DatasourceState.CREATING.equals(datasource.getState()) == false) {
            log.error("Invalid datasource state. Expecting {} but received {}", DatasourceState.CREATING, datasource.getState());
            markDatasourceAsCreateFailed(datasource);
            return;
        }

        try {
            datasourceUpdateService.updateOrCreateGeoIpData(datasource);
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
