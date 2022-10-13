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

import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseStringWithSuffix;
import static org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequestContent.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.rest.RestStatus;

public class RestUploadGeoJSONActionIT extends GeospatialRestTestCase {

    public static final int NUMBER_OF_FEATURES_TO_ADD = 3;

    public void testGeoJSONUploadSuccessPostMethod() throws Exception {

        final String index = randomLowerCaseStringWithSuffix(ACCEPTED_INDEX_SUFFIX_PATH);
        assertIndexNotExists(index);
        Response response = uploadGeoJSONFeatures(NUMBER_OF_FEATURES_TO_ADD, index, null);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        assertIndexExists(index);
        assertEquals("failed to index documents", NUMBER_OF_FEATURES_TO_ADD, getIndexDocumentCount(index));
    }

    public void testGeoJSONUploadFailIndexExists() throws IOException {

        String index = randomLowerCaseStringWithSuffix(ACCEPTED_INDEX_SUFFIX_PATH);
        ;
        String geoFieldName = randomLowerCaseString();
        Map<String, String> geoFields = new HashMap<>();
        geoFields.put(geoFieldName, "geo_shape");
        createIndex(index, Settings.EMPTY, geoFields);
        assertIndexExists(index);
        final ResponseException responseException = assertThrows(
            ResponseException.class,
            () -> uploadGeoJSONFeatures(NUMBER_OF_FEATURES_TO_ADD, index, geoFieldName)
        );
        assertTrue("Not an expected exception", responseException.getMessage().contains("resource_already_exists_exception"));
    }

    public void testGeoJSONUploadSuccessPutMethod() throws Exception {

        String index = randomLowerCaseStringWithSuffix(ACCEPTED_INDEX_SUFFIX_PATH);
        Response response = uploadGeoJSONFeaturesIntoExistingIndex(NUMBER_OF_FEATURES_TO_ADD, index, null);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        assertIndexExists(index);
        assertEquals("failed to index documents", NUMBER_OF_FEATURES_TO_ADD, getIndexDocumentCount(index));
    }

    public void testGeoJSONPutMethodUploadIndexExists() throws Exception {

        String index = randomLowerCaseStringWithSuffix(ACCEPTED_INDEX_SUFFIX_PATH);
        String geoFieldName = randomLowerCaseString();
        Response response = uploadGeoJSONFeaturesIntoExistingIndex(NUMBER_OF_FEATURES_TO_ADD, index, geoFieldName);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
        int indexDocumentCount = getIndexDocumentCount(index);
        // upload again
        Response uploadGeoJSONSecondTimeResponse = uploadGeoJSONFeaturesIntoExistingIndex(NUMBER_OF_FEATURES_TO_ADD, index, geoFieldName);
        assertEquals(RestStatus.OK, RestStatus.fromCode(uploadGeoJSONSecondTimeResponse.getStatusLine().getStatusCode()));
        int expectedDocCountAfterUpload = indexDocumentCount + NUMBER_OF_FEATURES_TO_ADD;
        assertEquals("failed to index documents", expectedDocCountAfterUpload, getIndexDocumentCount(index));
    }
}
