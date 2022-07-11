/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.geospatial.GeospatialTestHelper.buildFieldNameValuePair;
import static org.opensearch.geospatial.stats.upload.UploadStatsBuilder.randomUploadStats;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

public class TotalUploadStatsTests extends OpenSearchTestCase {
    private static final int MAX_STATS_COUNT = 5;
    private static final int MIN_STATS_COUNT = 2;

    private static final long INIT = 0L;

    public void testInstanceCreation() {
        int randomStatsCount = randomIntBetween(MIN_STATS_COUNT, MAX_STATS_COUNT);
        TotalUploadStats totalUploadStats = new TotalUploadStats(randomUploadStats(randomStatsCount));
        assertNotNull("Failed to create TotalUploadStats", totalUploadStats);
        assertFalse("Failed to add stats to list", totalUploadStats.isUploadStatsEmpty());
    }

    public void testInstanceCreationFails() {
        assertThrows(NullPointerException.class, () -> new TotalUploadStats(null));
    }

    public void testEmptyUploadStats() {
        TotalUploadStats totalUploadStats = new TotalUploadStats(Collections.emptyList());
        assertTrue("stats should be empty", totalUploadStats.isUploadStatsEmpty());
    }

    public void testToXContentWithEmptyUploadStats() throws IOException {
        TotalUploadStats totalUploadStats = new TotalUploadStats(Collections.emptyList());
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        contentBuilder.startObject();
        totalUploadStats.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String summary = Strings.toString(contentBuilder);
        assertNotNull(summary);
        String expectedEmptyContent = "{\"total\":{}}";
        assertEquals(expectedEmptyContent, summary);
    }

    public void testToXContentWithRequestAPICount() throws IOException {
        List<UploadStats> randomUploadStats = randomUploadStats(MAX_STATS_COUNT);
        long expectedSum = INIT;
        expectedSum += randomUploadStats.stream().mapToLong(UploadStats::getTotalAPICount).sum();
        TotalUploadStats totalUploadStats = new TotalUploadStats(randomUploadStats);
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        contentBuilder.startObject();
        totalUploadStats.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String summary = Strings.toString(contentBuilder);
        assertNotNull(summary);
        assertTrue(summary.contains(buildFieldNameValuePair(TotalUploadStats.FIELDS.REQUEST_COUNT.toString(), expectedSum)));
    }

    public void testToXContentWithUploadCount() throws IOException {
        List<UploadStats> randomUploadStats = randomUploadStats(MAX_STATS_COUNT);
        long expectedSum = INIT;
        for (UploadStats stats : randomUploadStats) {
            expectedSum += stats.getMetrics().stream().mapToLong(UploadMetric::getUploadCount).sum();
        }
        TotalUploadStats totalUploadStats = new TotalUploadStats(randomUploadStats);
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        contentBuilder.startObject();
        totalUploadStats.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String summary = Strings.toString(contentBuilder);
        assertNotNull(summary);
        assertTrue(summary.contains(buildFieldNameValuePair(TotalUploadStats.FIELDS.UPLOAD.toString(), expectedSum)));
    }

    public void testToXContentWithSuccessCount() throws IOException {
        List<UploadStats> randomUploadStats = randomUploadStats(MAX_STATS_COUNT);
        long expectedSum = INIT;
        for (UploadStats stats : randomUploadStats) {
            expectedSum += stats.getMetrics().stream().mapToLong(UploadMetric::getSuccessCount).sum();
        }
        TotalUploadStats totalUploadStats = new TotalUploadStats(randomUploadStats);
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        contentBuilder.startObject();
        totalUploadStats.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String summary = Strings.toString(contentBuilder);
        assertNotNull(summary);
        assertTrue(summary.contains(buildFieldNameValuePair(TotalUploadStats.FIELDS.SUCCESS.toString(), expectedSum)));
    }

    public void testToXContentWithFailedCount() throws IOException {
        List<UploadStats> randomUploadStats = randomUploadStats(MAX_STATS_COUNT);
        long expectedSum = INIT;
        for (UploadStats stats : randomUploadStats) {
            expectedSum += stats.getMetrics().stream().mapToLong(UploadMetric::getFailedCount).sum();
        }
        TotalUploadStats totalUploadStats = new TotalUploadStats(randomUploadStats);
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        contentBuilder.startObject();
        totalUploadStats.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String summary = Strings.toString(contentBuilder);
        assertNotNull(summary);
        assertTrue(summary.contains(buildFieldNameValuePair(TotalUploadStats.FIELDS.FAILED.toString(), expectedSum)));
    }

    public void testToXContentWithDuration() throws IOException {
        List<UploadStats> randomUploadStats = randomUploadStats(MAX_STATS_COUNT);
        long expectedSum = INIT;
        for (UploadStats stats : randomUploadStats) {
            expectedSum += stats.getMetrics().stream().mapToLong(UploadMetric::getDuration).sum();
        }
        TotalUploadStats totalUploadStats = new TotalUploadStats(randomUploadStats);
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        contentBuilder.startObject();
        totalUploadStats.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String summary = Strings.toString(contentBuilder);
        assertNotNull(summary);
        assertTrue(summary.contains(buildFieldNameValuePair(TotalUploadStats.FIELDS.DURATION.toString(), expectedSum)));
    }
}
