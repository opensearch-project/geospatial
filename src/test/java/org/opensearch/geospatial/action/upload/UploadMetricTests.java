/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload;

import static org.opensearch.geospatial.GeospatialTestHelper.buildFieldNameValuePair;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import org.opensearch.common.Strings;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class UploadMetricTests extends OpenSearchTestCase {

    private static final Integer MINIMUM_UPLOAD_DOCUMENT_COUNT = 1;

    public void testInstanceCreation() {
        String metricID = randomLowerCaseString();
        UploadMetric.UploadMetricBuilder builder = new UploadMetric.UploadMetricBuilder(metricID);

        int uploadCount = randomIntBetween(MINIMUM_UPLOAD_DOCUMENT_COUNT, Integer.MAX_VALUE);
        builder.uploadCount(uploadCount);

        int successCount = randomIntBetween(MINIMUM_UPLOAD_DOCUMENT_COUNT, uploadCount);
        builder.successCount(successCount);

        int failedCount = uploadCount - successCount;
        builder.failedCount(failedCount);

        long duration = randomNonNegativeLong();
        builder.duration(duration);

        UploadMetric actualMetric = builder.build();

        assertNotNull("metric cannot be null", actualMetric);
        assertEquals(uploadCount, actualMetric.getUploadCount());
        assertEquals(successCount, actualMetric.getSuccessCount());
        assertEquals(failedCount, actualMetric.getFailedCount());
        assertEquals(duration, actualMetric.getDuration());
        assertEquals(metricID, actualMetric.getMetricID());
    }

    public void testToXContent() {
        UploadMetric actualMetric = GeospatialTestHelper.generateRandomUploadMetric();
        String metricAsString = Strings.toString(actualMetric);
        assertNotNull(metricAsString);
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.ID, actualMetric.getMetricID())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.COUNT, actualMetric.getUploadCount())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.DURATION, actualMetric.getDuration())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.FAILED, actualMetric.getFailedCount())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.SUCCESS, actualMetric.getSuccessCount())));
    }
}
