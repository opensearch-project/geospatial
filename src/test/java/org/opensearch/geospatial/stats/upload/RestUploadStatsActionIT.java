/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;
import static org.opensearch.geospatial.stats.upload.RestUploadStatsAction.ACTION_OBJECT;
import static org.opensearch.geospatial.stats.upload.RestUploadStatsAction.ACTION_STATS;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.rest.RestStatus;

public class RestUploadStatsActionIT extends GeospatialRestTestCase {

    private static final int NUMBER_OF_FEATURES_TO_ADD = 3;

    private String getUploadStatsPath() {
        return String.join(URL_DELIMITER, getPluginURLPrefix(), ACTION_OBJECT, ACTION_STATS);
    }

    private String getStatsResponseAsString() throws Exception {
        Request statsRequest = new Request("GET", getUploadStatsPath());
        Response statsResponse = client().performRequest(statsRequest);
        return EntityUtils.toString(statsResponse.getEntity());
    }

    public void testStatsAPISuccess() throws Exception {

        Request request = new Request("GET", getUploadStatsPath());
        Response response = client().performRequest(request);
        assertEquals("Failed to retrieve stats", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    public void testStatsAreUpdatedAfterUpload() throws Exception {
        // get current stats response
        final String currentUploadStats = getStatsResponseAsString();
        assertNotNull(currentUploadStats);

        Response response = uploadGeoJSONFeatures(NUMBER_OF_FEATURES_TO_ADD, null, null);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // get stats response after an upload
        final String newUploadStats = getStatsResponseAsString();
        assertNotNull(newUploadStats);
        assertTrue("New metrics are not added", newUploadStats.length() > currentUploadStats.length());
    }

}
