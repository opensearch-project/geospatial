/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import java.io.IOException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;

public class StatsNodeResponseTests extends OpenSearchTestCase {

    public void testStream() throws IOException {
        StatsNodeResponse nodeResponse = StatsNodeResponseBuilder.randomStatsNodeResponse(GeospatialTestHelper.randomLowerCaseString());
        BytesStreamOutput output = new BytesStreamOutput();
        nodeResponse.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        StatsNodeResponse serializedNodeResponse = new StatsNodeResponse(in);
        assertNotNull("serialized node response cannot be null", serializedNodeResponse);
        assertArrayEquals(
            "mismatch metrics during serialization",
            nodeResponse.getUploadStats().getMetrics().toArray(),
            serializedNodeResponse.getUploadStats().getMetrics().toArray()
        );
        assertEquals(
            "mismatch api count",
            nodeResponse.getUploadStats().getTotalAPICount(),
            serializedNodeResponse.getUploadStats().getTotalAPICount()
        );
    }
}
