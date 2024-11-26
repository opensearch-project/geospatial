/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import java.util.Map;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionResponse;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

/**
 * Facade to provide GeoLocation enrichment for other plugins.
 */
@Log4j2
@AllArgsConstructor
public class IpEnrichmentActionClient {

    final private NodeClient nodeClient;

    /**
     * IpEnrichment with default datasource.
     * @param ipString Ip String to resolve.
     * @return A map instance which contain GeoLocation data for the given Ip address.
     */
    public Map<String, Object> getGeoLocationData (String ipString) {
        return getGeoLocationData(ipString, null);
    }

    /**
     * Client facing method, which read an IP in String form and return a map instance which contain the associated GeoLocation data.
     * @param ipString IP v4 || v6 address in String form.
     * @param datasourceName datasourceName in String form.
     * @return A map instance which contain GeoLocation data for the given Ip address.
     */
    public Map<String, Object> getGeoLocationData(String ipString, String datasourceName) {
        // Composite the request object.
        ActionFuture<ActionResponse> responseActionFuture = nodeClient.execute(
            IpEnrichmentAction.INSTANCE,
            new IpEnrichmentRequest(ipString, datasourceName)
        );
        // Send out the request and process the response.
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
