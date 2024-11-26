/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.core.action.ActionResponse;

import lombok.SneakyThrows;

public class IpEnrichmentActionClientTest {

    @Mock
    private NodeClient mockNodeClient;

    @Mock
    private ActionFuture<ActionResponse> mockResult;

    @SneakyThrows
    @Test
    public void testWithValidResponse() {
        Map<String, Object> dummyPayload = Map.of("k1", "v1");
        String dummyIpString = "192.168.1.1";
        when(mockResult.get()).thenReturn(new IpEnrichmentResponse(dummyPayload));
        when(mockNodeClient.execute(eq(IpEnrichmentAction.INSTANCE), any())).thenReturn(mockResult);

        IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(mockNodeClient);
        Map<String, Object> actualPayload = ipClient.getGeoLocationData(dummyIpString);
        Assert.assertEquals(dummyPayload, actualPayload);
    }

    @Test
    @SneakyThrows
    public void testWithException() {
        String dummyIpString = "192.168.1.1";
        when(mockResult.get()).thenThrow(new ExecutionException(new Throwable()));
        when(mockNodeClient.execute(eq(IpEnrichmentAction.INSTANCE), any())).thenReturn(mockResult);

        IpEnrichmentActionClient ipClient = new IpEnrichmentActionClient(mockNodeClient);
        Assert.assertNull(ipClient.getGeoLocationData(dummyIpString));
    }
}
