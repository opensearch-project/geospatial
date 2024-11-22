/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class IpEnrichmentResponseTest {

    /**
     * To simulate when Response class being passed from one plugin to the other.
     */
    @Test
    void testFromActionResponseWithValidPayload() {

        Map<String, Object> payload = Map.of("k1", "v1");
        IpEnrichmentResponse response = new IpEnrichmentResponse(payload);
        IpEnrichmentResponse castedResponse = IpEnrichmentResponse.fromActionResponse(response);
        Assertions.assertEquals(response.getGeoLocationData(), castedResponse.getGeoLocationData());
    }
}