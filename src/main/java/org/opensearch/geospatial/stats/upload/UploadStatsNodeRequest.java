/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class UploadStatsNodeRequest extends BaseNodeRequest {

    private UploadStatsRequest request;

    public UploadStatsNodeRequest() {
        super();
    }

    public UploadStatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new UploadStatsRequest(in);
    }

    public UploadStatsNodeRequest(UploadStatsRequest request) {
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

}
