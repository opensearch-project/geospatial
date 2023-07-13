/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

public class UploadStatsResponse extends BaseNodesResponse<UploadStatsNodeResponse> implements Writeable, ToXContentObject {

    public UploadStatsResponse(StreamInput in) throws IOException {
        super(new ClusterName(in), in.readList(UploadStatsNodeResponse::new), in.readList(FailedNodeException::new));
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

        final Map<String, UploadStats> nodeIDStatsMap = getNodes().stream()
            .collect(Collectors.toMap(response -> response.getNode().getId(), UploadStatsNodeResponse::getUploadStats));
        UploadStatsService uploadStatsService = new UploadStatsService(nodeIDStatsMap);
        return uploadStatsService.toXContent(builder, params);
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
