/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

public class RestUpdateDatasourceHandlerTests extends RestActionTestCase {
    private String path;
    private RestUpdateDatasourceHandler handler;

    @Before
    public void setupAction() {
        handler = new RestUpdateDatasourceHandler();
        controller().registerHandler(handler);
        path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/%s/_settings");
    }

    public void testPrepareRequest_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String content = "{\"endpoint\":\"https://test.com\", \"update_interval_in_days\":1}";
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
    }

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
    }
}
