/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static java.util.Collections.emptyList;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.GeospatialTestHelper.removeStartAndEndObject;
import static org.opensearch.geospatial.stats.upload.UploadStatsNodeResponseBuilder.randomStatsNodeResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class UploadStatsResponseTests extends OpenSearchTestCase {

    public void testXContentWithMetrics() throws IOException {

        Map<String, UploadStatsNodeResponse> nodeResponse = randomStatsNodeResponse();
        UploadStatsResponse uploadStatsResponse = new UploadStatsResponse(
            new ClusterName(randomLowerCaseString()),
            new ArrayList<>(nodeResponse.values()),
            emptyList()
        );
        final XContentBuilder serviceContentBuilder = jsonBuilder();
        uploadStatsResponse.toXContent(serviceContentBuilder, ToXContent.EMPTY_PARAMS);
        String nodesResponseAsString = serviceContentBuilder.toString();
        assertNotNull(nodesResponseAsString);

        final List<UploadMetric> uploadMetrics = getUploadMetrics(nodeResponse);
        for (UploadMetric metric : uploadMetrics) {
            XContentBuilder metricContent = XContentFactory.jsonBuilder().startObject();
            metric.toXContent(metricContent, ToXContent.EMPTY_PARAMS);
            metricContent.endObject();
            final String metricAsString = metricContent.toString();
            assertNotNull(metricAsString);
            assertTrue(nodesResponseAsString.contains(removeStartAndEndObject(metricAsString)));
        }
    }

    public void testXContentWithTotalUploads() throws IOException {

        Map<String, UploadStatsNodeResponse> nodeResponse = randomStatsNodeResponse();
        UploadStatsResponse uploadStatsResponse = new UploadStatsResponse(
            new ClusterName(randomLowerCaseString()),
            new ArrayList<>(nodeResponse.values()),
            emptyList()
        );
        final XContentBuilder serviceContentBuilder = jsonBuilder();
        uploadStatsResponse.toXContent(serviceContentBuilder, ToXContent.EMPTY_PARAMS);
        String nodesResponseAsString = serviceContentBuilder.toString();
        assertNotNull(nodesResponseAsString);

        TotalUploadStats totalUploadStats = new TotalUploadStats(getUploadStats(nodeResponse));
        XContentBuilder totalUploadStatsContent = XContentFactory.jsonBuilder().startObject();
        totalUploadStats.toXContent(totalUploadStatsContent, ToXContent.EMPTY_PARAMS);
        totalUploadStatsContent.endObject();
        final String totalUploadStatsAsString = totalUploadStatsContent.toString();
        assertNotNull(totalUploadStatsAsString);
        assertTrue(nodesResponseAsString.contains(removeStartAndEndObject(totalUploadStatsAsString)));
    }

    public void testUploadStatsResponseStream() throws IOException {
        Map<String, UploadStatsNodeResponse> nodeResponse = randomStatsNodeResponse();
        UploadStatsResponse uploadStatsResponse = new UploadStatsResponse(
            new ClusterName(randomLowerCaseString()),
            new ArrayList<>(nodeResponse.values()),
            emptyList()
        );
        BytesStreamOutput output = new BytesStreamOutput();
        uploadStatsResponse.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        UploadStatsResponse serializedResponse = new UploadStatsResponse(in);
        assertNotNull("serialized response cannot be null", serializedResponse);
    }

    private List<UploadStats> getUploadStats(Map<String, UploadStatsNodeResponse> nodeResponse) {
        return nodeResponse.values().stream().map(UploadStatsNodeResponse::getUploadStats).collect(Collectors.toList());
    }

    private List<UploadMetric> getUploadMetrics(Map<String, UploadStatsNodeResponse> nodeResponse) {
        return nodeResponse.values()
            .stream()
            .map(UploadStatsNodeResponse::getUploadStats)
            .map(UploadStats::getMetrics)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

}
