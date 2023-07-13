/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class UploadStatsTransportAction extends TransportNodesAction<
    UploadStatsRequest,
    UploadStatsResponse,
    UploadStatsNodeRequest,
    UploadStatsNodeResponse> {

    private final TransportService transportService;
    private final UploadStats uploadStats;

    @Inject
    public UploadStatsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        UploadStats uploadStats
    ) {
        super(
            UploadStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            UploadStatsRequest::new,
            UploadStatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            UploadStatsNodeResponse.class
        );
        this.transportService = transportService;
        this.uploadStats = uploadStats;
    }

    @Override
    protected UploadStatsResponse newResponse(
        UploadStatsRequest nodesRequest,
        List<UploadStatsNodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new UploadStatsResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected UploadStatsNodeRequest newNodeRequest(UploadStatsRequest nodesRequest) {
        return new UploadStatsNodeRequest(nodesRequest);
    }

    @Override
    protected UploadStatsNodeResponse newNodeResponse(StreamInput streamInput) throws IOException {
        return new UploadStatsNodeResponse(streamInput);
    }

    @Override
    protected UploadStatsNodeResponse nodeOperation(UploadStatsNodeRequest nodeRequest) {
        return new UploadStatsNodeResponse(transportService.getLocalNode(), uploadStats);
    }
}
