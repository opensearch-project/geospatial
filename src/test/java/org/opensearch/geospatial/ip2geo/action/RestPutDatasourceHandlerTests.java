/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;

import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

@SuppressForbidden(reason = "unit test")
public class RestPutDatasourceHandlerTests extends RestActionTestCase {
    private String path;
    private RestPutDatasourceHandler action;

    @Before
    public void setupAction() {
        action = new RestPutDatasourceHandler(new ClusterSettings(Settings.EMPTY, new HashSet(Ip2GeoSettings.settings())));
        controller().registerHandler(action);
        path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/%s");
    }

    public void testPrepareRequest() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String content = "{\"endpoint\":\"https://test.com\", \"update_interval_in_days\":1}";
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(String.format(Locale.ROOT, path, datasourceName))
            .withContent(new BytesArray(content), XContentType.JSON)
            .build();
        AtomicBoolean isExecuted = new AtomicBoolean(false);

        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof PutDatasourceRequest);
            PutDatasourceRequest putDatasourceRequest = (PutDatasourceRequest) actionRequest;
            assertEquals("https://test.com", putDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(1), putDatasourceRequest.getUpdateInterval());
            assertEquals(datasourceName, putDatasourceRequest.getName());
            isExecuted.set(true);
            return null;
        });

        dispatchRequest(request);
        assertTrue(isExecuted.get());
    }

    public void testPrepareRequestDefaultValue() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath(String.format(Locale.ROOT, path, datasourceName))
            .withContent(new BytesArray("{}"), XContentType.JSON)
            .build();
        AtomicBoolean isExecuted = new AtomicBoolean(false);
        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof PutDatasourceRequest);
            PutDatasourceRequest putDatasourceRequest = (PutDatasourceRequest) actionRequest;
            assertEquals("https://geoip.maps.opensearch.org/v1/geolite2-city/manifest.json", putDatasourceRequest.getEndpoint());
            assertEquals(TimeValue.timeValueDays(3), putDatasourceRequest.getUpdateInterval());
            assertEquals(datasourceName, putDatasourceRequest.getName());
            isExecuted.set(true);
            return null;
        });

        dispatchRequest(request);
        assertTrue(isExecuted.get());
    }
}
