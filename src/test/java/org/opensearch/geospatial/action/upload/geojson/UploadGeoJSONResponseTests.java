/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.Strings;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONResponseTests extends OpenSearchTestCase {

    private static final int MAX_SUCCESS_ITEM_COUNT = 5;
    private static final int MIN_SUCCESS_ITEM_COUNT = 5;
    private static final int FAILURE_ITEM_COUNT = 1;

    public void testToXContentHasNoFailure() {
        int successActionCount = randomIntBetween(MIN_SUCCESS_ITEM_COUNT, MAX_SUCCESS_ITEM_COUNT);
        final BulkResponse bulkItemResponses = GeospatialTestHelper.generateRandomBulkResponse(successActionCount, false);
        UploadGeoJSONResponse getResponse = new UploadGeoJSONResponse(bulkItemResponses);
        String responseBody = Strings.toString(getResponse);
        assertTrue(responseBody.contains("\"errors\":false"));
        assertTrue(responseBody.contains("\"failure\":0"));
        assertTrue(responseBody.contains("\"total\":" + successActionCount));
        assertTrue(responseBody.contains("\"success\":" + successActionCount));

    }

    public void testToXContentHasFailure() {
        int successActionCount = randomIntBetween(MIN_SUCCESS_ITEM_COUNT, MAX_SUCCESS_ITEM_COUNT);
        int totalActionCount = successActionCount + FAILURE_ITEM_COUNT;
        final BulkResponse bulkItemResponses = GeospatialTestHelper.generateRandomBulkResponse(successActionCount, true);
        UploadGeoJSONResponse getResponse = new UploadGeoJSONResponse(bulkItemResponses);
        String responseBody = Strings.toString(getResponse);
        assertTrue(responseBody.contains("\"errors\":true"));
        assertTrue(responseBody.contains("\"total\":" + totalActionCount));
        assertTrue(responseBody.contains("\"success\":" + successActionCount));
        assertTrue(responseBody.contains("\"failure\":" + FAILURE_ITEM_COUNT));
    }
}
