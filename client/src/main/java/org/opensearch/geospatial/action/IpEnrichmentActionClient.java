/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionResponse;

import java.util.concurrent.ExecutionException;

/**
 * Proxy for the node client operations.
 */
public class IpEnrichmentActionClient {

    NodeClient nodeClient;

    public IpEnrichmentActionClient(NodeClient nodeClient) {
        this.nodeClient = nodeClient;
    }

    public String enrichIp(String ipString) throws ExecutionException, InterruptedException {
        ActionFuture<ActionResponse> responseActionFuture = nodeClient.execute(IpEnrichmentAction.INSTANCE, new IpEnrichmentRequest(ipString));
        ActionResponse genericActionResponse = responseActionFuture.get();
        IpEnrichmentResponse enrichmentResponse = IpEnrichmentResponse.fromActionResponse(genericActionResponse);
        return enrichmentResponse.getAnswer();
    }

}
