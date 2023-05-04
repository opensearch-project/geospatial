/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

public class RestGetDatasourceHandlerTests extends RestActionTestCase {
    private String PATH_FOR_ALL = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource");
    private String path;
    private RestGetDatasourceHandler action;

    @Before
    public void setupAction() {
        action = new RestGetDatasourceHandler();
        controller().registerHandler(action);
        path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/%s");
    }

    public void testPrepareRequest_whenNames_thenSucceed() {
        String dsName1 = GeospatialTestHelper.randomLowerCaseString();
        String dsName2 = GeospatialTestHelper.randomLowerCaseString();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(String.format(Locale.ROOT, path, StringUtils.joinWith(",", dsName1, dsName2)))
            .build();

        AtomicBoolean isExecuted = new AtomicBoolean(false);
        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            // Verifying
            assertTrue(actionRequest instanceof GetDatasourceRequest);
            GetDatasourceRequest getDatasourceRequest = (GetDatasourceRequest) actionRequest;
            assertArrayEquals(new String[] { dsName1, dsName2 }, getDatasourceRequest.getNames());
            isExecuted.set(true);
            return null;
        });

        // Run
        dispatchRequest(request);

        // Verify
        assertTrue(isExecuted.get());
    }

    public void testPrepareRequest_whenAll_thenSucceed() {
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath(PATH_FOR_ALL)
            .build();

        AtomicBoolean isExecuted = new AtomicBoolean(false);
        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            // Verifying
            assertTrue(actionRequest instanceof GetDatasourceRequest);
            GetDatasourceRequest getDatasourceRequest = (GetDatasourceRequest) actionRequest;
            assertArrayEquals(new String[] {}, getDatasourceRequest.getNames());
            isExecuted.set(true);
            return null;
        });

        // Run
        dispatchRequest(request);

        // Verify
        assertTrue(isExecuted.get());
    }
}
