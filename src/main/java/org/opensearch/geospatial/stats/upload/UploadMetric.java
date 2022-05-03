/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

/**
 * UploadMetric stores metric for an upload API
 */
public final class UploadMetric implements ToXContentFragment, Writeable {

    public enum FIELDS {
        UPLOAD,
        DURATION,
        FAILED,
        ID,
        SUCCESS,
        TYPE;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.getDefault());
        }
    }

    private final long duration;
    private final long failedCount;
    private final String metricID;
    private final long successCount;
    private final long uploadCount;

    private final String type;

    private UploadMetric(UploadMetricBuilder builder) {
        this.metricID = builder.metricID;
        this.uploadCount = builder.uploadCount;
        this.successCount = builder.successCount;
        this.failedCount = builder.failedCount;
        this.duration = builder.duration;
        this.type = builder.type;
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
     * @return Total time spent in milliseconds to upload, this includes both
     * succeeded and failed time
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @return Geospatial object's type
     */
    public String getType() {
        return type;
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELDS.ID.toString(), metricID);
        builder.field(FIELDS.TYPE.toString(), type);
        builder.field(FIELDS.UPLOAD.toString(), uploadCount);
        builder.field(FIELDS.SUCCESS.toString(), successCount);
        builder.field(FIELDS.FAILED.toString(), failedCount);
        builder.field(FIELDS.DURATION.toString(), duration);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput output) throws IOException {
        output.writeString(metricID);
        output.writeString(type);
        output.writeVLong(uploadCount);
        output.writeVLong(successCount);
        output.writeVLong(failedCount);
        output.writeVLong(duration);
    }

    /**
     * Builder to create {@link UploadMetric}
     */
    public static final class UploadMetricBuilder {

        private long duration;
        private long failedCount;
        private final String metricID;
        private final String type;
        private long successCount;
        private long uploadCount;

        public UploadMetricBuilder(String metricID, String type) {
            if (!Strings.hasText(metricID)) {
                throw new IllegalArgumentException("metric ID cannot be empty");
            }
            if (!Strings.hasText(type)) {
                throw new IllegalArgumentException("type cannot be empty");
            }
            this.metricID = metricID;
            this.type = type;
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

        /**
         * Deserialize {@link UploadMetric} from given {@link StreamInput}
         * @param input StreamInput instance
         * @return UploadMetric returns {@link UploadMetric} from {@link StreamInput}
         * @throws IOException if unable to read from {@link StreamInput}
         */
        public static UploadMetric fromStreamInput(StreamInput input) throws IOException {
            String metricId = input.readString();
            String type = input.readString();
            UploadMetricBuilder builder = new UploadMetricBuilder(metricId, type).uploadCount(input.readVLong())
                .successCount(input.readVLong())
                .failedCount(input.readVLong())
                .duration(input.readVLong());
            return builder.build();
        }

    }
}
