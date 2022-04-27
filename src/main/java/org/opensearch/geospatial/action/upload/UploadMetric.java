/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload;

import java.util.Objects;

import org.opensearch.common.Strings;

/**
 * UploadMetric stores metric for an upload API
 */
public final class UploadMetric {

    private final long duration;
    private final long failedCount;
    private final String metricID;
    private final long successCount;
    private final long uploadCount;

    private UploadMetric(UploadMetricBuilder builder) {
        this.metricID = builder.metricID;
        this.uploadCount = builder.uploadCount;
        this.successCount = builder.successCount;
        this.failedCount = builder.failedCount;
        this.duration = builder.duration;
    }

    /**
     * @return Total number of documents that are failed to upload
     */
    public long getFailedCount() {
        return failedCount;
    }

    /**
     * @return Total number of documents that are successfully uploaded
     */
    public long getSuccessCount() {
        return successCount;
    }

    /**
     * @return Total number of documents to be uploaded
     */
    public long getUploadCount() {
        return uploadCount;
    }

    /**
     * @return Metric's Identifier
     */
    public String getMetricID() {
        return metricID;
    }

    /**
     * @return Total time spent in milli seconds to upload, this includes both
     * succeeded and failed time
     */
    public long getDuration() {
        return duration;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UploadMetric newMetric = (UploadMetric) o;
        return Objects.equals(metricID, newMetric.metricID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricID);
    }

    /**
     * Builder to create {@link UploadMetric}
     */
    public static final class UploadMetricBuilder {

        private long duration;
        private long failedCount;
        private String metricID;
        private long successCount;
        private long uploadCount;

        public UploadMetricBuilder(String metricID) {
            if (!Strings.hasText(metricID)) {
                throw new IllegalArgumentException("metric ID cannot be empty");
            }
            this.metricID = metricID;
        }

        public UploadMetricBuilder uploadCount(long uploadCount) {
            this.uploadCount = uploadCount;
            return this;
        }

        public UploadMetricBuilder successCount(long successCount) {
            this.successCount = successCount;
            return this;
        }

        public UploadMetricBuilder failedCount(long failedCount) {
            this.failedCount = failedCount;
            return this;
        }

        public UploadMetricBuilder duration(long duration) {
            this.duration = duration;
            return this;
        }

        /**
         * @return UploadMetric instance from the builder
         */
        public UploadMetric build() {
            return new UploadMetric(this);
        }

    }
}
