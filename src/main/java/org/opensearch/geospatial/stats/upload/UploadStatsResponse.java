/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class UploadStatsResponse extends BaseNodesResponse<UploadStatsNodeResponse> implements Writeable, ToXContentObject {

    private final static String UPLOAD = "upload";

    protected UploadStatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public UploadStatsResponse(ClusterName clusterName, List<UploadStatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<UploadStatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(UploadStatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<UploadStatsNodeResponse> nodeResponses) throws IOException {
        out.writeList(nodeResponses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(UPLOAD);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UploadStatsResponse otherResponse = (UploadStatsResponse) o;
        return Objects.equals(getNodes(), otherResponse.getNodes()) && Objects.equals(failures(), otherResponse.failures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), failures());
    }

}
