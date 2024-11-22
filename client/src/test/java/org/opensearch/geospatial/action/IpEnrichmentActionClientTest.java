/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionResponse;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IpEnrichmentActionClientTest {

    // Test with happy path

    // Test with exception path

    @Mock
    private NodeClient mockNodeClient;

    @Mock
    private ActionFuture<ActionResponse> mockResult;

    @SneakyThrows
    @Test
    void testWithValidResponse() {
        Map<String, Object> dummyPayload = Map.of("k1", "v1");
        String dummyIpString = "192.168.1.1";
        when(mockResult.get()).thenReturn(new IpEnrichmentResponse(dummyPayload));
        when(mockNodeClient.execute(eq(IpEnrichmentAction.INSTANCE), any())).thenReturn(mockResult);

        IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(mockNodeClient);
        Map<String, Object> actualPayload = ipClient.getGeoLocationData(dummyIpString);
        Assertions.assertEquals(dummyPayload, actualPayload);
    }

    @SneakyThrows
    @Test
    void testWithException() {
        Map<String, Object> dummyPayload = Map.of("k1", "v1");
        String dummyIpString = "192.168.1.1";
        when(mockResult.get()).thenThrow(new ExecutionException(new Throwable()));
        when(mockNodeClient.execute(eq(IpEnrichmentAction.INSTANCE), any())).thenReturn(mockResult);

        IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(mockNodeClient);
        Assertions.assertNull(ipClient.getGeoLocationData(dummyIpString));
    }
}