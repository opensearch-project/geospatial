/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import java.io.IOException;

import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.rest.OpenSearchRestTestCase;

public class GeospatialPluginIT extends OpenSearchRestTestCase {

    /**
     * Tests whether plugin is installed or not
     */
    public void testPluginInstalled() throws IOException {
        String restURI = String.join("/", "_cat", "plugins");

        Request request = new Request("GET", restURI);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);
        assertTrue(responseBody.contains("opensearch-geospatial"));
    }
}
