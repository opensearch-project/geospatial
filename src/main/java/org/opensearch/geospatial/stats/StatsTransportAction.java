/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;
import java.util.List;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public class StatsTransportAction extends TransportNodesAction<StatsRequest, StatsResponse, StatsNodeRequest, StatsNodeResponse> {

    private final TransportService transportService;
    private final UploadStats uploadStats;

    @Inject
    public StatsTransportAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        UploadStats uploadStats
    ) {
        super(
            StatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            StatsRequest::new,
            StatsNodeRequest::new,
            ThreadPool.Names.MANAGEMENT,
            StatsNodeResponse.class
        );
        this.transportService = transportService;
        this.uploadStats = uploadStats;
    }

    @Override
    protected StatsResponse newResponse(
        StatsRequest nodesRequest,
        List<StatsNodeResponse> nodeResponses,
        List<FailedNodeException> failures
    ) {
        return new StatsResponse(clusterService.getClusterName(), nodeResponses, failures);
    }

    @Override
    protected StatsNodeRequest newNodeRequest(StatsRequest nodesRequest) {
        return new StatsNodeRequest(nodesRequest);
    }

    @Override
    protected StatsNodeResponse newNodeResponse(StreamInput streamInput) throws IOException {
        return new StatsNodeResponse(streamInput);
    }

    @Override
    protected StatsNodeResponse nodeOperation(StatsNodeRequest nodeRequest) {
        return new StatsNodeResponse(transportService.getLocalNode(), uploadStats);
    }
}
