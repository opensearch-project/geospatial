/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodeRequest;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

public class StatsNodeRequest extends BaseNodeRequest {

    private StatsRequest request;

    public StatsNodeRequest() {
        super();
    }

    public StatsNodeRequest(StreamInput in) throws IOException {
        super(in);
        request = new StatsRequest(in);
    }

    public StatsNodeRequest(StatsRequest request) {
        this.request = request;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        request.writeTo(out);
    }

}
