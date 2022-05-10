/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.geospatial.GeospatialTestHelper.buildFieldNameValuePair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensearch.common.Strings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class UploadStatsServiceTests extends OpenSearchTestCase {

    private String removeStartAndEndObject(String content) {
        assertNotNull(content);
        assertTrue("content length should be at least 2", content.length() > 1);
        return content.substring(1, content.length() - 1);
    }

    public void testInstanceCreation() {
        Map<String, UploadStats> randomMap = new HashMap<>();
        randomMap.put(GeospatialTestHelper.randomLowerCaseString(), UploadStatsBuilder.randomUploadStats());
        randomMap.put(GeospatialTestHelper.randomLowerCaseString(), UploadStatsBuilder.randomUploadStats());

        UploadStatsService service = new UploadStatsService(randomMap);
        assertNotNull(service);
    }

    public void testInstanceCreationFails() {
        assertThrows(NullPointerException.class, () -> new UploadStatsService(null));
    }

    public void testXContentWithNodeID() {
        Map<String, UploadStats> randomMap = new HashMap<>();
        randomMap.put(GeospatialTestHelper.randomLowerCaseString(), UploadStatsBuilder.randomUploadStats());
        randomMap.put(GeospatialTestHelper.randomLowerCaseString(), UploadStatsBuilder.randomUploadStats());
        UploadStatsService service = new UploadStatsService(randomMap);
        String xContent = Strings.toString(service);
        assertNotNull(xContent);
        for (String nodeID : randomMap.keySet()) {
            assertTrue(nodeID + " is missing", xContent.contains(buildFieldNameValuePair(UploadStatsService.NODE_ID, nodeID)));
        }
    }

    public void testXContentWithEmptyStats() throws IOException {
        UploadStatsService service = new UploadStatsService(new HashMap<>());
        final XContentBuilder contentBuilder = jsonBuilder().startObject();
        service.toXContent(contentBuilder, ToXContent.EMPTY_PARAMS);
        contentBuilder.endObject();
        String emptyContent = "{\"uploads\":{\"total\":{},\"metrics\":[]}}";
        assertEquals(emptyContent, Strings.toString(contentBuilder));
    }

    public void testXContentWithTotalUploadStats() throws IOException {
        Map<String, UploadStats> randomMap = new HashMap<>();
        final List<UploadStats> uploadStats = List.of(UploadStatsBuilder.randomUploadStats(), UploadStatsBuilder.randomUploadStats());

        for (UploadStats stats : uploadStats) {
            randomMap.put(GeospatialTestHelper.randomLowerCaseString(), stats);
        }
        UploadStatsService service = new UploadStatsService(randomMap);
        final XContentBuilder serviceContentBuilder = jsonBuilder().startObject();
        service.toXContent(serviceContentBuilder, ToXContent.EMPTY_PARAMS);
        serviceContentBuilder.endObject();
        String content = Strings.toString(serviceContentBuilder);
        assertNotNull(content);

        final XContentBuilder summary = jsonBuilder().startObject();
        TotalUploadStats expectedSummary = new TotalUploadStats(uploadStats);
        expectedSummary.toXContent(summary, ToXContent.EMPTY_PARAMS);
        summary.endObject();
        final String totalUploadStatsSummary = Strings.toString(summary);
        assertNotNull(totalUploadStatsSummary);
        assertTrue(content.contains(removeStartAndEndObject(totalUploadStatsSummary)));
    }

    public void testXContentWithMetrics() throws IOException {
        Map<String, UploadStats> randomMap = new HashMap<>();
        List<UploadMetric> randomMetrics = new ArrayList<>();
        final List<UploadStats> uploadStats = List.of(UploadStatsBuilder.randomUploadStats(), UploadStatsBuilder.randomUploadStats());
        for (UploadStats stats : uploadStats) {
            randomMap.put(GeospatialTestHelper.randomLowerCaseString(), stats);
            randomMetrics.addAll(stats.getMetrics());
        }
        UploadStatsService service = new UploadStatsService(randomMap);
        final XContentBuilder serviceContentBuilder = jsonBuilder().startObject();
        service.toXContent(serviceContentBuilder, ToXContent.EMPTY_PARAMS);
        serviceContentBuilder.endObject();
        String content = Strings.toString(serviceContentBuilder);
        assertNotNull(content);

        for (UploadMetric metric : randomMetrics) {
            XContentBuilder metricsAsContent = jsonBuilder().startObject();
            metric.toXContent(metricsAsContent, ToXContent.EMPTY_PARAMS);
            metricsAsContent.endObject();
            final String metricsAsString = Strings.toString(metricsAsContent);
            assertNotNull(metricsAsString);
            assertTrue(content.contains(removeStartAndEndObject(metricsAsString)));
        }
    }
}
