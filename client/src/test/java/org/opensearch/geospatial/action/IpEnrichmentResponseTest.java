/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class IpEnrichmentResponseTest {

    /**
     * To simulate when Response class being passed from one plugin to the other.
     */
    @Test
    public void testFromActionResponseWithValidPayload() {

        Map<String, Object> payload = Map.of("k1", "v1");
        IpEnrichmentResponse response = new IpEnrichmentResponse(payload);
        IpEnrichmentResponse castedResponse = IpEnrichmentResponse.fromActionResponse(response);
        Assert.assertEquals(response.getGeoLocationData(), castedResponse.getGeoLocationData());
    }
}
