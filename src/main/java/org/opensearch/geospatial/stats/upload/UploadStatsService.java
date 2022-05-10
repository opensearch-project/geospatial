/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

// Service to calculate summary of upload stats and generate XContent for StatsResponse
public class UploadStatsService implements ToXContentFragment {

    public static final String UPLOADS = "uploads";
    public static final String TOTAL = "total";
    public static final String METRICS = "metrics";
    public static final String NODE_ID = "node_id";
    private final Map<String, UploadStats> uploadStats;
    private final TotalUploadStats totalUploadStats;

    public UploadStatsService(Map<String, UploadStats> uploadStats) {
        this.uploadStats = Objects.requireNonNull(uploadStats, "upload stats map cannot be null");
        this.totalUploadStats = new TotalUploadStats(new ArrayList<>(uploadStats.values()));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        /*
        {
          "uploads": {
            "total": {
                request_count : # of request,
                "upload"       : sum of documents to upload across API,
                "success"     : sum of successfully uploaded documents across API,
                "failed"      : sum of failed to upload documents across API,
                "duration"    : sum of duration in milliseconds to ingest document across API
            },
            "metrics" : [
                {
                    "id"       : <metric-id>,
                    "node_id"  : node-id
                    "upload"   : # of documents to upload,
                    "success"  : # of successfully uploaded documents,
                    "failed"   : # of failed to upload documents,
                    "duration" : duration in milliseconds to ingest document
                }, ......
            ]
          }
        }
         */
        builder.startObject(UPLOADS);
        totalUploadStats.toXContent(builder, params);
        builder.startArray(METRICS);
        if (totalUploadStats.isUploadStatsEmpty()) {
            builder.endArray();
            return builder.endObject();
        }
        final List<AbstractMap.SimpleEntry<String, UploadMetric>> metricsByNodeID = groupMetricsByNodeID(uploadStats);
        for (AbstractMap.SimpleEntry<String, UploadMetric> entry : metricsByNodeID) {
            addMetrics(builder, params, entry);
        }
        builder.endArray();
        return builder.endObject();
    }

    private void addMetrics(XContentBuilder builder, Params params, AbstractMap.SimpleEntry<String, UploadMetric> entry)
        throws IOException {
        builder.startObject();
        builder.field(NODE_ID, entry.getKey());
        entry.getValue().toXContent(builder, params);
        builder.endObject();
    }

    private List<AbstractMap.SimpleEntry<String, UploadMetric>> groupMetricsByNodeID(Map<String, UploadStats> uploadStats) {
        return uploadStats.entrySet()
            .stream()
            .flatMap(stat -> stat.getValue().getMetrics().stream().map(metric -> new AbstractMap.SimpleEntry<>(stat.getKey(), metric)))
            .collect(Collectors.toList());

    }
}
