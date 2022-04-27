/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload;

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
        assertTrue(metricAsString.contains(actualMetric.getMetricID()));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELD_NAMES.count, actualMetric.getUploadCount())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELD_NAMES.duration, actualMetric.getDuration())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELD_NAMES.failed, actualMetric.getFailedCount())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELD_NAMES.success, actualMetric.getSuccessCount())));
    }

    private StringBuilder buildFieldNameValuePair(UploadMetric.FIELD_NAMES field, long value) {
        StringBuilder builder = new StringBuilder();
        builder.append("\"").append(field).append("\":").append(value);
        return builder;
    }

}
