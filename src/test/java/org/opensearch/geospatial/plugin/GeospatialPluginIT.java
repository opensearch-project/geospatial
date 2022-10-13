/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.rest.RestStatus;

public class GeospatialPluginIT extends GeospatialRestTestCase {

    /**
     * Tests whether plugin is installed or not
     */
    public void testPluginInstalled() throws Exception {
        String restURI = String.join("/", "_cat", "plugins");

        Request request = new Request("GET", restURI);
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        String responseBody = EntityUtils.toString(response.getEntity());
        assertNotNull(responseBody);
        assertTrue(responseBody.contains("opensearch-geospatial"));
    }
}
