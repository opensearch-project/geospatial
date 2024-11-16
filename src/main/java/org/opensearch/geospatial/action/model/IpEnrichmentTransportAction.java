/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.model;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.geospatial.action.IpEnrichmentAction;
import org.opensearch.geospatial.action.IpEnrichmentRequest;
import org.opensearch.geospatial.action.IpEnrichmentResponse;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class IpEnrichmentTransportAction extends HandledTransportAction<ActionRequest,
        ActionResponse> {


    @Inject
    public IpEnrichmentTransportAction(
            TransportService transportService,
            ActionFilters actionFilters) {
        super(IpEnrichmentAction.NAME, transportService, actionFilters, IpEnrichmentRequest::new);
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        IpEnrichmentRequest enrichmentRequest = IpEnrichmentRequest.fromActionRequest(request);
        listener.onResponse(new IpEnrichmentResponse(enrichmentRequest.getIpString() + " Done!"));
    }


//    @Override
//    protected void doExecute(Task task, ActionRequest request, ActionListener<IpEnrichmentResponse> listener) {
//        IpEnrichmentRequest enrichmentRequest = IpEnrichmentRequest.fromActionRequest(request);
//        listener.onResponse(new IpEnrichmentResponse(enrichmentRequest.getIpString() + " Done!"));
//    }



}
