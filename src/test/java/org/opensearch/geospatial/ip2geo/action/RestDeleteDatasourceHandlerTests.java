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
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.test.rest.RestActionTestCase;

public class RestDeleteDatasourceHandlerTests extends RestActionTestCase {
    private String path;
    private RestDeleteDatasourceHandler action;

    @Before
    public void setupAction() {
        action = new RestDeleteDatasourceHandler();
        controller().registerHandler(action);
        path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/%s");
    }

    public void testPrepareRequest_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.DELETE)
            .withPath(String.format(Locale.ROOT, path, datasourceName))
            .build();
        AtomicBoolean isExecuted = new AtomicBoolean(false);

        verifyingClient.setExecuteLocallyVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof DeleteDatasourceRequest);
            DeleteDatasourceRequest deleteDatasourceRequest = (DeleteDatasourceRequest) actionRequest;
            assertEquals(datasourceName, deleteDatasourceRequest.getName());
            isExecuted.set(true);
            return null;
        });

        dispatchRequest(request);
        assertTrue(isExecuted.get());
    }
}
