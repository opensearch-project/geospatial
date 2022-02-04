/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.rest.action.upload.geojson;

import java.io.IOException;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.rest.RestStatus;

public class RestUploadGeoJSONActionIT extends GeospatialRestTestCase {

    public void testGeoJSONUploadSuccessPostMethod() throws IOException {

        String path = String.join(
            RestUploadGeoJSONAction.URL_DELIMITER,
            GeospatialPlugin.getPluginURLPrefix(),
            RestUploadGeoJSONAction.ACTION_OBJECT,
            RestUploadGeoJSONAction.ACTION_UPLOAD
        );
        Request request = new Request("POST", path);
        request.setJsonEntity(buildUploadGeoJSONRequestContent().toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

    }

    public void testGeoJSONUploadSuccessPutMethod() throws IOException {

        String path = String.join(
            RestUploadGeoJSONAction.URL_DELIMITER,
            GeospatialPlugin.getPluginURLPrefix(),
            RestUploadGeoJSONAction.ACTION_OBJECT,
            RestUploadGeoJSONAction.ACTION_UPLOAD
        );
        Request request = new Request("PUT", path);
        request.setJsonEntity(buildUploadGeoJSONRequestContent().toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));

    }
}
