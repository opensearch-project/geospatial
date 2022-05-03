/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

public class UploadStatsNodeResponse extends BaseNodeResponse implements ToXContentFragment {

    public UploadStatsNodeResponse(DiscoveryNode node, UploadStats stats) {
        super(node);
    }

    public UploadStatsNodeResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(Boolean.TRUE);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) {
        return builder;
    }
}
