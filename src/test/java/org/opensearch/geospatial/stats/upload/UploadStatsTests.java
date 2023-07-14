/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.geospatial.GeospatialTestHelper.GEOJSON;
import static org.opensearch.geospatial.GeospatialTestHelper.buildFieldNameValuePair;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class UploadStatsTests extends OpenSearchTestCase {

    private static final int NO_API_CALLED = 0;
    private static final int MIN_API_CALLED = 1;
    private static final int MAX_API_CALLED = 5;

    public void testGetInstance() {
        UploadStats stats = new UploadStats();
        assertNotNull("metric cannot be null", stats);
    }

    public void testAddMetricSuccess() {
        UploadStats stats = new UploadStats();
        assertEquals(NO_API_CALLED, stats.getTotalAPICount());
        int metricCount = randomIntBetween(MIN_API_CALLED, MAX_API_CALLED);
        Set<UploadMetric> expectedMetrics = new HashSet<>();
        IntStream.rangeClosed(MIN_API_CALLED, metricCount).forEach(unUsed -> {
            UploadMetric randomMetric = GeospatialTestHelper.generateRandomUploadMetric();
            expectedMetrics.add(randomMetric);
            stats.addMetric(randomMetric);
            stats.incrementAPICount();
        });
        assertArrayEquals(expectedMetrics.toArray(), stats.getMetrics().toArray());
        assertEquals(metricCount, stats.getTotalAPICount());
    }

    public void testAddMetricFailsForNullValue() {
        UploadStats stats = new UploadStats();
        assertThrows("null metric cannot be added", NullPointerException.class, () -> stats.addMetric(null));
    }

    public void testAddMetricFailsForNoUploadCount() {
        UploadStats stats = new UploadStats();
        UploadMetric.UploadMetricBuilder emptyBuilder = new UploadMetric.UploadMetricBuilder(
            GeospatialTestHelper.randomLowerCaseString(),
            GEOJSON
        );
        assertThrows("metric without upload cannot be added", IllegalArgumentException.class, () -> stats.addMetric(emptyBuilder.build()));
    }

    public void testAddMetricFailsForDuplicateMetrics() {
        UploadStats stats = new UploadStats();
        UploadMetric randomMetric = GeospatialTestHelper.generateRandomUploadMetric();
        stats.addMetric(randomMetric);
        assertThrows("duplicate metrics are not allowed", IllegalArgumentException.class, () -> stats.addMetric(randomMetric));
    }

    public void testStreams() throws IOException {
        UploadStats stats = new UploadStats();
        int metricCount = randomIntBetween(MIN_API_CALLED, MAX_API_CALLED);
        Set<UploadMetric> expectedMetrics = new HashSet<>();
        IntStream.rangeClosed(MIN_API_CALLED, metricCount).forEach(unUsed -> {
            UploadMetric randomMetric = GeospatialTestHelper.generateRandomUploadMetric();
            expectedMetrics.add(randomMetric);
            stats.addMetric(randomMetric);
            stats.incrementAPICount();
        });
        BytesStreamOutput output = new BytesStreamOutput();
        stats.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);
        UploadStats serializedStats = UploadStats.fromStreamInput(in);
        assertNotNull("serialized stats cannot be null", serializedStats);
        assertEquals("api count is ", stats.getTotalAPICount(), serializedStats.getTotalAPICount());
        assertEquals("failed to serialize metrics", stats.getMetrics().size(), serializedStats.getMetrics().size());
    }

    public void testToXContent() throws IOException {
        UploadStats stats = UploadStatsBuilder.randomUploadStats();
        XContentBuilder statsContent = XContentFactory.jsonBuilder().startObject();
        String statsAsString = Strings.toString(stats.toXContent(statsContent, ToXContent.EMPTY_PARAMS).endObject());
        assertNotNull(statsAsString);
        assertTrue(statsAsString.contains(buildFieldNameValuePair(UploadStats.FIELDS.REQUEST_COUNT.toString(), stats.getTotalAPICount())));
        stats.getMetrics().forEach(uploadMetric -> {
            assertTrue(statsAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.TYPE, GEOJSON)));
            assertTrue(statsAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.UPLOAD, uploadMetric.getUploadCount())));
            assertTrue(statsAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.DURATION, uploadMetric.getDuration())));
            assertTrue(statsAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.FAILED, uploadMetric.getFailedCount())));
            assertTrue(statsAsString.contains(buildFieldNameValuePair(UploadMetric.FIELDS.SUCCESS, uploadMetric.getSuccessCount())));
        });
    }
}
