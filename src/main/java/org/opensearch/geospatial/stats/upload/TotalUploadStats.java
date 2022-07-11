/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Function;

import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

// Holder to construct summary of Upload API Stats across all Nodes
public final class TotalUploadStats implements ToXContentObject {

    // XContent field names
    public enum FIELDS {
        DURATION,
        FAILED,
        REQUEST_COUNT,
        SUCCESS,
        TOTAL,
        UPLOAD;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.getDefault());
        }
    }

    private final List<UploadStats> uploadStatsList;

    public TotalUploadStats(final List<UploadStats> uploadStatsList) {
        this.uploadStatsList = Objects.requireNonNull(uploadStatsList, "Upload stats list cannot be null");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(FIELDS.TOTAL.toString());
        if (isUploadStatsEmpty()) {
            return builder.endObject();
        }
        long totalRequestCount = uploadStatsList.stream().mapToLong(UploadStats::getTotalAPICount).sum();
        builder.field(FIELDS.REQUEST_COUNT.toString(), totalRequestCount);

        long totalUpload = sumMetricField(uploadStatsList, UploadMetric::getUploadCount);
        builder.field(FIELDS.UPLOAD.toString(), totalUpload);

        long totalSuccess = sumMetricField(uploadStatsList, UploadMetric::getSuccessCount);
        builder.field(FIELDS.SUCCESS.toString(), totalSuccess);

        long totalFailed = sumMetricField(uploadStatsList, UploadMetric::getFailedCount);
        builder.field(FIELDS.FAILED.toString(), totalFailed);

        long totalDuration = sumMetricField(uploadStatsList, UploadMetric::getDuration);
        builder.field(FIELDS.DURATION.toString(), totalDuration);
        return builder.endObject();
    }

    private long sumMetricField(List<UploadStats> stats, Function<UploadMetric, Long> mapper) {
        return stats.stream().map(UploadStats::getMetrics).flatMap(List::stream).mapToLong(mapper::apply).sum();
    }

    /**
     * Return whether any upload metrics are available or not
     * @return true if no stats are available
     */
    public boolean isUploadStatsEmpty() {
        return uploadStatsList.isEmpty();
    }

}
