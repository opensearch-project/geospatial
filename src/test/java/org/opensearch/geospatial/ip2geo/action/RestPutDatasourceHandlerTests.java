/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.util.HashSet;

import org.junit.Before;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

public class RestPutDatasourceHandlerTests extends RestActionTestCase {
    private RestPutDatasourceHandler action;

    @Before
    public void setupAction() {
        action = new RestPutDatasourceHandler(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, new HashSet(Ip2GeoSettings.settings())));
        controller().registerHandler(action);
    }

    public void testPrepareRequest() {
        String content = "{\"endpoint\":\"https://test.com\", \"update_interval\":1}";
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_geoip/datasource/test")
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof PutDatasourceRequest);
            PutDatasourceRequest putDatasourceRequest = (PutDatasourceRequest) actionRequest;
            assertEquals("https://test.com", putDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(1), putDatasourceRequest.getUpdateIntervalInDays());
            assertEquals("test", putDatasourceRequest.getDatasourceName());
            return null;
        });

        dispatchRequest(restRequest);
    }

    public void testPrepareRequestDefaultValue() {
        RestRequest restRequestWithEmptyContent = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_geoip/datasource/test")
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();

        RestRequest restRequestWithoutContent = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_geoip/datasource/test")
            .build();

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof PutDatasourceRequest);
            PutDatasourceRequest putDatasourceRequest = (PutDatasourceRequest) actionRequest;
            assertEquals("https://geoip.maps.opensearch.org/v1/geolite-2/manifest.json", putDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(3), putDatasourceRequest.getUpdateIntervalInDays());
            assertEquals("test", putDatasourceRequest.getDatasourceName());
            return null;
        });

        dispatchRequest(restRequestWithEmptyContent);
        dispatchRequest(restRequestWithoutContent);
    }
}
