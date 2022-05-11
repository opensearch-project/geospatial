/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.geospatial.stats.upload.UploadStatsService;

public class StatsResponse extends BaseNodesResponse<StatsNodeResponse> implements Writeable, ToXContentObject {

    protected StatsResponse(StreamInput in) throws IOException {
        super(in);
    }

    public StatsResponse(ClusterName clusterName, List<StatsNodeResponse> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    @Override
    protected List<StatsNodeResponse> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(StatsNodeResponse::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<StatsNodeResponse> nodeResponses) throws IOException {
        super.writeTo(out);
        out.writeList(nodeResponses);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        final Map<String, UploadStats> nodeIDStatsMap = getNodes().stream()
            .collect(Collectors.toMap(response -> response.getNode().getId(), StatsNodeResponse::getUploadStats));
        UploadStatsService uploadStatsService = new UploadStatsService(nodeIDStatsMap);
        builder.startObject();
        uploadStatsService.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsResponse otherResponse = (StatsResponse) o;
        return Objects.equals(getNodes(), otherResponse.getNodes()) && Objects.equals(failures(), otherResponse.failures());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getNodes(), failures());
    }

}
