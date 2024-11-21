/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionResponse;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Facade to provide GeoLocation enrichment for other plugin.
 */
@Log4j2
@AllArgsConstructor
public class IpEnrichmentActionClient {

    NodeClient nodeClient;


    /**
     * IpEnrichment with default datasource.
     * @param ipString Ip String to resolve.
     * @return A map instance which contain GeoLocation data for the given Ip address.
     */
    public Map<String, Object> getGeoLocationData (String ipString) {
        return getGeoLocationData(ipString, "defaultDataSource");
    }

    /**
     * Client facing method, which read an IP in String form and return a map instance which contain the associated GeoLocation data.
     * @param ipString IP v4 || v6 address in String form.
     * @param datasourceName datasourceName in String form.
     * @return A map instance which contain GeoLocation data for the given Ip address.
     */
    public Map<String, Object> getGeoLocationData (String ipString, String datasourceName) {
        ActionFuture<ActionResponse> responseActionFuture = nodeClient.execute(
                IpEnrichmentAction.INSTANCE, new IpEnrichmentRequest(ipString, datasourceName));
        try {
            ActionResponse genericActionResponse = responseActionFuture.get();
            IpEnrichmentResponse enrichmentResponse = IpEnrichmentResponse.fromActionResponse(genericActionResponse);
            return enrichmentResponse.getGeoLocationData();
        } catch (Exception e) {
            log.error("GeoSpatial IP Enrichment call failure, with detail: ", e);
            return null;
        }
    }
}
