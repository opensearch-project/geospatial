/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;

public class StatsRequest extends BaseNodesRequest<StatsRequest> {

    /**
     * Empty constructor needed for StatsTransportAction
     */
    public StatsRequest() {
        super((String[]) null);
    }

    protected StatsRequest(StreamInput in) throws IOException {
        super(in);
    }
}
