/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import java.io.IOException;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.common.io.stream.StreamInput;

public class UploadStatsRequest extends BaseNodesRequest<UploadStatsRequest> {

    /**
     * Empty constructor needed for StatsTransportAction
     */
    public UploadStatsRequest() {
        super((String[]) null);
    }

    protected UploadStatsRequest(StreamInput in) throws IOException {
        super(in);
    }
}
