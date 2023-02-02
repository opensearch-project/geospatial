/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

/**
 * Contains the total upload stats
 */
public final class UploadStats implements Writeable, ToXContentObject {

    private static final UploadStats instance = new UploadStats();

    public enum FIELDS {
        METRICS,
        REQUEST_COUNT;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Set<UploadMetric> metrics;
    private final CounterMetric totalAPICount;

    /**
     * @return Singleton instance of UploadStats
     */
    public static UploadStats getInstance() {
        return instance;
    }

    UploadStats() {
        metrics = new HashSet<>();
        totalAPICount = new CounterMetric();
    }

    /**
     * Get UploadStats from {@link StreamInput}.
     * @param input contains {@link UploadStats} in serialized form
     * @return UploadStats instance
     * @throws IOException if cannot read {@link UploadStats} from given input
     */
    public static UploadStats fromStreamInput(StreamInput input) throws IOException {
        Objects.requireNonNull(input, "StreamInput cannot be null");
        UploadStats instance = new UploadStats();
        instance.totalAPICount.inc(input.readVLong());
        instance.metrics.addAll(input.readSet(UploadMetric.UploadMetricBuilder::fromStreamInput));
        return instance;
    }

    /**
     * Add new metric to {@link UploadStats}
     * @param newMetric {@link UploadMetric} to be added to Stats
     */
    public void addMetric(UploadMetric newMetric) {
        Objects.requireNonNull(newMetric, "metric cannot be null");
        if (metrics.contains(newMetric)) {
            throw new IllegalArgumentException(newMetric.getMetricID() + " already exists");
        }
        if (newMetric.getUploadCount() < 1) {
            throw new IllegalArgumentException("metric should have at least 1 upload");
        }
        metrics.add(newMetric);
    }

    /**
     * Increment API Count
     */
    public void incrementAPICount() {
        totalAPICount.inc();
    }

    /**
     * Get total number of times Upload API is called
     * @return value of totalAPICount
     */
    public long getTotalAPICount() {
        return totalAPICount.count();
    }

    /**
     * Get list of added metrics so far to stats
     * @return List of {@link UploadMetric}
     */
    public List<UploadMetric> getMetrics() {
        return List.copyOf(metrics);
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeVLong(getTotalAPICount());
        output.writeCollection(metrics);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELDS.REQUEST_COUNT.toString(), getTotalAPICount());
        builder.startArray(FIELDS.METRICS.toString());
        for (UploadMetric metric : metrics) {
            builder.startObject();
            metric.toXContent(builder, params);
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }
}
