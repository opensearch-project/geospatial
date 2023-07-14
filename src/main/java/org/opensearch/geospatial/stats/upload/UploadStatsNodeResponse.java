/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class UploadStatsNodeResponse extends BaseNodeResponse implements ToXContentObject {

    private static final String UPLOADS = "uploads";
    private final UploadStats uploadStats;

    public UploadStatsNodeResponse(DiscoveryNode node, UploadStats uploadStats) {
        super(node);
        this.uploadStats = Objects.requireNonNull(uploadStats, "upload stats cannot be null");
    }

    public UploadStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
        uploadStats = UploadStats.fromStreamInput(in);
    }

    public UploadStats getUploadStats() {
        return uploadStats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        uploadStats.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(UPLOADS);
        uploadStats.toXContent(builder, params);
        return builder.endObject();
    }
}
