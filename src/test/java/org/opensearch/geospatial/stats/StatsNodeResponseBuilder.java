/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.stats.upload.UploadStatsBuilder.randomUploadStats;
import static org.opensearch.test.OpenSearchTestCase.buildNewFakeTransportAddress;
import static org.opensearch.test.OpenSearchTestCase.randomIntBetween;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;

public class StatsNodeResponseBuilder {
    private static final int MIN_STATS_NODE_RESPONSE = 2;
    private static final int MAX_STATS_NODE_RESPONSE = 5;

    public static StatsNodeResponse randomStatsNodeResponse(String nodeID) {
        DiscoveryNode node = new DiscoveryNode(
            randomLowerCaseString(),
            nodeID,
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            Version.CURRENT
        );
        return new StatsNodeResponse(node, randomUploadStats());
    }

    public static Map<String, StatsNodeResponse> randomStatsNodeResponse() {
        int randomResponseCount = randomIntBetween(MIN_STATS_NODE_RESPONSE, MAX_STATS_NODE_RESPONSE);
        Map<String, StatsNodeResponse> nodesResponseMap = new HashMap<>(randomResponseCount);
        IntStream.range(0, randomResponseCount).forEach(unused -> {
            String nodeID = randomLowerCaseString();
            nodesResponseMap.put(nodeID, randomStatsNodeResponse(nodeID));
        });
        return nodesResponseMap;
    }
}
