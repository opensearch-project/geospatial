/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import static org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction.ACTION_UPLOAD;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;
import static org.opensearch.geospatial.stats.RestStatsAction.ACTION_OBJECT;

import java.io.IOException;

import org.apache.http.util.EntityUtils;
import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.rest.RestStatus;

public class RestStatsActionIT extends GeospatialRestTestCase {

    private static final int NUMBER_OF_FEATURES_TO_ADD = 3;

    private String getStatsResponseAsString() throws IOException {
        String statsPath = String.join(URL_DELIMITER, getPluginURLPrefix(), ACTION_OBJECT);
        Request statsRequest = new Request("GET", statsPath);
        Response statsResponse = client().performRequest(statsRequest);
        return EntityUtils.toString(statsResponse.getEntity());
    }

    public void testStatsAPISuccess() throws IOException {

        String path = String.join(URL_DELIMITER, getPluginURLPrefix(), ACTION_OBJECT);
        Request request = new Request("GET", path);
        Response response = client().performRequest(request);
        assertEquals("Failed to retrieve stats", RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

    public void testStatsAreUpdatedAfterUpload() throws IOException {
        // get current stats response
        final String currentUploadStats = getStatsResponseAsString();
        assertNotNull(currentUploadStats);

        // upload geoJSON
        String path = String.join(URL_DELIMITER, getPluginURLPrefix(), RestUploadGeoJSONAction.ACTION_OBJECT, ACTION_UPLOAD);
        Request request = new Request("POST", path);
        final JSONObject requestBody = buildUploadGeoJSONRequestContent(NUMBER_OF_FEATURES_TO_ADD, null, null);
        request.setJsonEntity(requestBody.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

        // get stats response after an upload
        final String newUploadStats = getStatsResponseAsString();
        assertNotNull(newUploadStats);
        assertTrue("New metrics are not added", newUploadStats.length() > currentUploadStats.length());
    }

}
