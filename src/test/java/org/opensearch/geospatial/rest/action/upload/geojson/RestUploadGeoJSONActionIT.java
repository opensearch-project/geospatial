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

import static org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction.ACTION_OBJECT;
import static org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction.ACTION_UPLOAD;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.rest.RestStatus;

public class RestUploadGeoJSONActionIT extends GeospatialRestTestCase {

    public static final int NUMBER_OF_FEATURES_TO_ADD = 3;

    public void testGeoJSONUploadSuccessPostMethod() throws IOException {

        String path = String.join(GeospatialPlugin.URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        Request request = new Request("POST", path);
        final JSONObject requestBody = buildUploadGeoJSONRequestContent(NUMBER_OF_FEATURES_TO_ADD, null, null);
        final String index = requestBody.getString(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName());
        assertIndexNotExists(index);
        request.setJsonEntity(requestBody.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        assertIndexExists(index);
        assertEquals("failed to index documents", NUMBER_OF_FEATURES_TO_ADD, getIndexDocumentCount(index));
    }

    public void testGeoJSONUploadFailIndexExists() throws IOException {

        String index = randomLowerCaseString();
        String geoFieldName = randomLowerCaseString();
        Map<String, String> geoFields = new HashMap<>();
        geoFields.put(geoFieldName, "geo_shape");
        createIndex(index, Settings.EMPTY, geoFields);
        String path = String.join(GeospatialPlugin.URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        Request request = new Request("POST", path);
        final JSONObject requestBody = buildUploadGeoJSONRequestContent(NUMBER_OF_FEATURES_TO_ADD, index, geoFieldName);
        request.setJsonEntity(requestBody.toString());
        final ResponseException responseException = assertThrows(ResponseException.class, () -> client().performRequest(request));
        assertTrue("Not an expected exception", responseException.getMessage().contains("resource_already_exists_exception"));
    }

    public void testGeoJSONUploadSuccessPutMethod() throws IOException {

        String path = String.join(GeospatialPlugin.URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        Request request = new Request("PUT", path);
        final JSONObject requestBody = buildUploadGeoJSONRequestContent(NUMBER_OF_FEATURES_TO_ADD, null, null);
        final String index = requestBody.getString(UploadGeoJSONRequestContent.FIELD_INDEX.getPreferredName());
        assertIndexNotExists(index);
        request.setJsonEntity(requestBody.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        assertIndexExists(index);
        assertEquals("failed to index documents", NUMBER_OF_FEATURES_TO_ADD, getIndexDocumentCount(index));
    }

    public void testGeoJSONPutMethodUploadIndexExists() throws IOException {

        String index = randomLowerCaseString();
        String geoFieldName = randomLowerCaseString();
        String path = String.join(GeospatialPlugin.URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        Request request = new Request("PUT", path);
        final JSONObject requestBody = buildUploadGeoJSONRequestContent(NUMBER_OF_FEATURES_TO_ADD, index, geoFieldName);
        request.setJsonEntity(requestBody.toString());
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        int indexDocumentCount = getIndexDocumentCount(index);
        // upload again
        final JSONObject requestBodyUpload = buildUploadGeoJSONRequestContent(NUMBER_OF_FEATURES_TO_ADD, index, geoFieldName);
        request.setJsonEntity(requestBodyUpload.toString());
        Response uploadGeoJSONSecondTimeResponse = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(uploadGeoJSONSecondTimeResponse.getStatusLine().getStatusCode()));
        int expectedDocCountAfterUpload = indexDocumentCount + NUMBER_OF_FEATURES_TO_ADD;
        assertEquals("failed to index documents", expectedDocCountAfterUpload, getIndexDocumentCount(index));
    }
}
