/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import lombok.SneakyThrows;

import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class DeleteDatasourceRequestTests extends Ip2GeoTestCase {
    @SneakyThrows
    public void testStreamInOut_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        DeleteDatasourceRequest request = new DeleteDatasourceRequest(datasourceName);

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        DeleteDatasourceRequest copiedRequest = new DeleteDatasourceRequest(input);

        // Verify
        assertEquals(request.getName(), copiedRequest.getName());
    }
}
