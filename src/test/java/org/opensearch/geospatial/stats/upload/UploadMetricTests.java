/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.geospatial.GeospatialTestHelper.GEOJSON;
import static org.opensearch.geospatial.GeospatialTestHelper.buildFieldNameValuePair;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.io.IOException;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class UploadMetricTests extends OpenSearchTestCase {

    private static final Integer MINIMUM_UPLOAD_DOCUMENT_COUNT = 1;

    public void testInstanceCreation() {
        String metricID = randomLowerCaseString();
        UploadMetric.UploadMetricBuilder builder = new UploadMetric.UploadMetricBuilder(metricID, GEOJSON);

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
        assertEquals(GEOJSON, actualMetric.getType());
    }

    public void testInstanceFailsDueToNullID() {
        assertThrows(IllegalArgumentException.class, () -> new UploadMetric.UploadMetricBuilder(null, GEOJSON));
    }

    public void testInstanceFailsDueToEmptypID() {
        assertThrows(IllegalArgumentException.class, () -> new UploadMetric.UploadMetricBuilder(new String(), GEOJSON));
    }

    public void testInstanceFailsDueToNullType() {
        String metricID = randomLowerCaseString();
        assertThrows(IllegalArgumentException.class, () -> new UploadMetric.UploadMetricBuilder(metricID, null));
    }

    public void testInstanceFailsDueToEmptyType() {
        String metricID = randomLowerCaseString();
        assertThrows(IllegalArgumentException.class, () -> new UploadMetric.UploadMetricBuilder(metricID, new String()));
    }

    public void testToXContent() {
        UploadMetric actualMetric = GeospatialTestHelper.generateRandomUploadMetric();
        String metricAsString = Strings.toString(XContentType.JSON, actualMetric);
        assertNotNull(metricAsString);
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.ID, actualMetric.getMetricID())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.TYPE, GEOJSON)));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.UPLOAD, actualMetric.getUploadCount())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.DURATION, actualMetric.getDuration())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.FAILED, actualMetric.getFailedCount())));
        assertTrue(metricAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.SUCCESS, actualMetric.getSuccessCount())));
    }

    public void testStreams() throws IOException {
        UploadMetric actualMetric = GeospatialTestHelper.generateRandomUploadMetric();
        BytesStreamOutput output = new BytesStreamOutput();
        actualMetric.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        UploadMetric serializedMetric = UploadMetric.UploadMetricBuilder.fromStreamInput(in);
        assertNotNull("serialized metric cannot be null", serializedMetric);
        assertEquals(
            "upload count is not matching between serialized and deserialized",
            actualMetric.getUploadCount(),
            serializedMetric.getUploadCount()
        );
        assertEquals(
            "success count is not matching between serialized and deserialized",
            actualMetric.getSuccessCount(),
            serializedMetric.getSuccessCount()
        );
        assertEquals(
            "failed count is not matching between serialized and deserialized",
            actualMetric.getFailedCount(),
            serializedMetric.getFailedCount()
        );
        assertEquals(
            "duration is not matching between serialized and deserialized",
            actualMetric.getDuration(),
            serializedMetric.getDuration()
        );
        assertEquals(
            "metric id is not matching between serialized and deserialized",
            actualMetric.getMetricID(),
            serializedMetric.getMetricID()
        );
        assertEquals(
            "geospatial type is not matching between serialized and deserialized",
            actualMetric.getType(),
            serializedMetric.getType()
        );
    }

}
