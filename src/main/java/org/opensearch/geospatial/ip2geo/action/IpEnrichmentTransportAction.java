/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import lombok.extern.log4j.Log4j2;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.geospatial.action.IpEnrichmentAction;
import org.opensearch.geospatial.action.IpEnrichmentRequest;
import org.opensearch.geospatial.action.IpEnrichmentResponse;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoCachedDao;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Transport action to convert provided IP address String into GeoLocation data.
 */
@Log4j2
public class IpEnrichmentTransportAction extends HandledTransportAction<ActionRequest,
        ActionResponse> {

    private Ip2GeoCachedDao ip2GeoCachedDao;

    /**
     * Constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param cachedDao the cached datasource facade
     */
    @Inject
    public IpEnrichmentTransportAction(
            TransportService transportService,
            ActionFilters actionFilters,
            Ip2GeoCachedDao cachedDao) {
        super(IpEnrichmentAction.NAME, transportService, actionFilters, IpEnrichmentRequest::new);
        this.ip2GeoCachedDao = cachedDao;
    }

    @Override
    protected void doExecute(Task task, ActionRequest request, ActionListener<ActionResponse> listener) {
        IpEnrichmentRequest enrichmentRequest = IpEnrichmentRequest.fromActionRequest(request);
        String ipString = enrichmentRequest.getIpString();
        Map<String, Object> testResult = ip2GeoCachedDao.getGeoData(".geospatial-ip2geo-data.my-datasource.ef3486f8-401b-4d77-b89b-3a4cd19eda04", ipString);
        System.out.println(testResult);
        listener.onResponse(new IpEnrichmentResponse(enrichmentRequest.getIpString() + " Done!"));
    }





}
