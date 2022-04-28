/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload;

import static org.opensearch.geospatial.GeospatialTestHelper.buildFieldNameValuePair;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import org.opensearch.common.Strings;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class UploadStatsTests extends OpenSearchTestCase {

    private static final int NO_API_CALLED = 0;
    private static final int MIN_API_CALLED = 1;
    private static final int MAX_API_CALLED = 5;
    public static final String METRICS_DELIMITER = ",";

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
        UploadMetric.UploadMetricBuilder emptyBuilder = new UploadMetric.UploadMetricBuilder(GeospatialTestHelper.randomLowerCaseString());
        assertThrows("metric without upload cannot be added", IllegalArgumentException.class, () -> stats.addMetric(emptyBuilder.build()));
    }

    public void testAddMetricFailsForDuplicateMetrics() {
        UploadStats stats = new UploadStats();
        UploadMetric randomMetric = GeospatialTestHelper.generateRandomUploadMetric();
        stats.addMetric(randomMetric);
        assertThrows("duplicate metrics are not allowed", IllegalArgumentException.class, () -> stats.addMetric(randomMetric));
    }

    public void testToXContent() {
        UploadStats stats = new UploadStats();
        int metricCount = randomIntBetween(MIN_API_CALLED, MAX_API_CALLED);
        Set<UploadMetric> expectedMetrics = new HashSet<>();
        IntStream.rangeClosed(MIN_API_CALLED, metricCount).forEach(unUsed -> {
            UploadMetric randomMetric = GeospatialTestHelper.generateRandomUploadMetric();
            expectedMetrics.add(randomMetric);
            stats.addMetric(randomMetric);
            stats.incrementAPICount();
        });
        String uploadStatsAsString = Strings.toString(stats);
        assertNotNull(uploadStatsAsString);
        assertTrue(uploadStatsAsString.contains(UploadStats.FIELDS.UPLOAD.toString()));
        assertTrue(uploadStatsAsString.contains(buildFieldNameValuePair(UploadStats.FIELDS.TOTAL.toString(), stats.getTotalAPICount())));
        assertTrue(uploadStatsAsString.contains(UploadStats.FIELDS.METRICS.toString()));
        expectedMetrics.stream()
            .map(Strings::toString)
            .forEach(metricAsString -> { assertTrue(uploadStatsAsString.contains(metricAsString)); });
    }
}
