/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import static java.util.Collections.emptyList;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.GeospatialTestHelper.removeStartAndEndObject;
import static org.opensearch.geospatial.stats.StatsNodeResponseBuilder.randomStatsNodeResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.cluster.ClusterName;
import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.geospatial.stats.upload.TotalUploadStats;
import org.opensearch.geospatial.stats.upload.UploadMetric;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.test.OpenSearchTestCase;

public class StatsResponseTests extends OpenSearchTestCase {

    public void testXContentWithMetrics() throws IOException {

        Map<String, StatsNodeResponse> nodeResponse = randomStatsNodeResponse();
        StatsResponse statsResponse = new StatsResponse(
            new ClusterName(randomLowerCaseString()),
            new ArrayList<>(nodeResponse.values()),
            emptyList()
        );
        final XContentBuilder serviceContentBuilder = jsonBuilder();
        statsResponse.toXContent(serviceContentBuilder, ToXContent.EMPTY_PARAMS);
        String nodesResponseAsString = Strings.toString(serviceContentBuilder);
        assertNotNull(nodesResponseAsString);

        final List<UploadMetric> uploadMetrics = getUploadMetrics(nodeResponse);
        for (UploadMetric metric : uploadMetrics) {
            XContentBuilder metricContent = XContentFactory.jsonBuilder().startObject();
            metric.toXContent(metricContent, ToXContent.EMPTY_PARAMS);
            metricContent.endObject();
            final String metricAsString = Strings.toString(metricContent);
            assertNotNull(metricAsString);
            assertTrue(nodesResponseAsString.contains(removeStartAndEndObject(metricAsString)));
        }
    }

    public void testXContentWithTotalUploads() throws IOException {

        Map<String, StatsNodeResponse> nodeResponse = randomStatsNodeResponse();
        StatsResponse statsResponse = new StatsResponse(
            new ClusterName(randomLowerCaseString()),
            new ArrayList<>(nodeResponse.values()),
            emptyList()
        );
        final XContentBuilder serviceContentBuilder = jsonBuilder();
        statsResponse.toXContent(serviceContentBuilder, ToXContent.EMPTY_PARAMS);
        String nodesResponseAsString = Strings.toString(serviceContentBuilder);
        assertNotNull(nodesResponseAsString);

        TotalUploadStats totalUploadStats = new TotalUploadStats(getUploadStats(nodeResponse));
        XContentBuilder totalUploadStatsContent = XContentFactory.jsonBuilder().startObject();
        totalUploadStats.toXContent(totalUploadStatsContent, ToXContent.EMPTY_PARAMS);
        totalUploadStatsContent.endObject();
        final String totalUploadStatsAsString = Strings.toString(totalUploadStatsContent);
        assertNotNull(totalUploadStatsAsString);
        assertTrue(nodesResponseAsString.contains(removeStartAndEndObject(totalUploadStatsAsString)));
    }

    private List<UploadStats> getUploadStats(Map<String, StatsNodeResponse> nodeResponse) {
        return nodeResponse.values().stream().map(StatsNodeResponse::getUploadStats).collect(Collectors.toList());
    }

    private List<UploadMetric> getUploadMetrics(Map<String, StatsNodeResponse> nodeResponse) {
        return nodeResponse.values()
            .stream()
            .map(StatsNodeResponse::getUploadStats)
            .map(UploadStats::getMetrics)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

}
