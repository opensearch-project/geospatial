/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.common.URLDenyListChecker;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

import lombok.SneakyThrows;

public class RestUpdateDatasourceHandlerTests extends RestActionTestCase {
    private String path;
    private RestUpdateDatasourceHandler handler;
    private URLDenyListChecker urlDenyListChecker;

    @Before
    public void setupAction() {
        urlDenyListChecker = mock(URLDenyListChecker.class);
        handler = new RestUpdateDatasourceHandler(urlDenyListChecker);
        controller().registerHandler(handler);
        path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/%s/_settings");
    }

    @SneakyThrows
    public void testPrepareRequest_whenValidInput_thenSucceed() {
        String endpoint = "https://test.com";
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String content = String.format(Locale.ROOT, "{\"endpoint\":\"%s\", \"update_interval_in_days\":1}", endpoint);
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(String.format(Locale.ROOT, path, datasourceName))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        AtomicBoolean isExecuted = new AtomicBoolean(false);

        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof UpdateDatasourceRequest);
            UpdateDatasourceRequest updateDatasourceRequest = (UpdateDatasourceRequest) actionRequest;
            assertEquals("https://test.com", updateDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(1), updateDatasourceRequest.getUpdateInterval());
            assertEquals(datasourceName, updateDatasourceRequest.getName());
            isExecuted.set(true);
            return null;
        });

        dispatchRequest(request);
        assertTrue(isExecuted.get());
        verify(urlDenyListChecker).toUrlIfNotInDenyList(endpoint);
    }

    @SneakyThrows
    public void testPrepareRequest_whenNullInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String content = "{}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(String.format(Locale.ROOT, path, datasourceName))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        AtomicBoolean isExecuted = new AtomicBoolean(false);

        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof UpdateDatasourceRequest);
            UpdateDatasourceRequest updateDatasourceRequest = (UpdateDatasourceRequest) actionRequest;
            assertNull(updateDatasourceRequest.getEndpoint());
            assertNull(updateDatasourceRequest.getUpdateInterval());
            assertEquals(datasourceName, updateDatasourceRequest.getName());
            isExecuted.set(true);
            return null;
        });

        dispatchRequest(request);
        assertTrue(isExecuted.get());
        verify(urlDenyListChecker, never()).toUrlIfNotInDenyList(anyString());
    }
}
