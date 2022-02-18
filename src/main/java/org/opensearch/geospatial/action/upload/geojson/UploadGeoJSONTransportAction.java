/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.util.Map;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.GeospatialParser;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * TransportAction to handle import operation
 */
public class UploadGeoJSONTransportAction extends HandledTransportAction<UploadGeoJSONRequest, AcknowledgedResponse> {

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public UploadGeoJSONTransportAction(
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client
    ) {
        super(UploadGeoJSONAction.NAME, transportService, actionFilters, UploadGeoJSONRequest::new);
        this.clusterService = clusterService;
        this.client = client;
    }

    @Override
    protected void doExecute(Task task, UploadGeoJSONRequest request, ActionListener<AcknowledgedResponse> actionListener) {
        // 1. parse request's data and extract into UploadGeoJSONRequestContent
        UploadGeoJSONRequestContent content = getContent(request, actionListener);
        if (content == null) {
            return;
        }

        // 2. Check should we continue upload if index exist.
        boolean failIfIndexExist = shouldFailIfIndexExist(request.getMethod());
        final boolean indexExists = clusterService.state().getRoutingTable().hasIndex(content.getIndexName());
        if (indexExists && failIfIndexExist) {
            actionListener.onFailure(new ResourceAlreadyExistsException(content.getIndexName()));
            return;
        }
        IndexManager indexManager = new IndexManager(client.admin().indices());
        PipelineManager pipelineManager = new PipelineManager(client.admin().cluster());
        ContentBuilder contentBuilder = new ContentBuilder(client);
        Uploader uploader = new Uploader(indexManager, pipelineManager, contentBuilder);
        // 3. upload GeoJSON as index document.
        try {
            uploader.upload(content, indexExists, actionListener);
        } catch (Exception e) { // doExecute should not throw any Exception since it is executed asynchronously
            actionListener.onFailure(e);
        }
    }

    private UploadGeoJSONRequestContent getContent(UploadGeoJSONRequest request, ActionListener<AcknowledgedResponse> actionListener) {
        Map<String, Object> contentAsMap = GeospatialParser.convertToMap(request.getContent());
        try {
            return UploadGeoJSONRequestContent.create(contentAsMap);
        } catch (Exception e) {
            actionListener.onFailure(e);
            return null;
        }
    }

    /*
     * Uploader needs to know whether to continue upload if index doesn't exist.
     * If method is POST, then, request should fail if index exist.
     */
    private boolean shouldFailIfIndexExist(RestRequest.Method method) {
        return RestRequest.Method.POST.equals(method);
    }
}
