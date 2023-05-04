/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class GetDatasourceRequestTests extends Ip2GeoTestCase {
    public void testStreamInOut_whenEmptyNames_thenSucceed() throws Exception {
        String[] names = new String[0];
        GetDatasourceRequest request = new GetDatasourceRequest(names);
        assertNull(request.validate());

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        GetDatasourceRequest copiedRequest = new GetDatasourceRequest(input);

        // Verify
        assertArrayEquals(request.getNames(), copiedRequest.getNames());
    }

    public void testStreamInOut_whenNames_thenSucceed() throws Exception {
        String[] names = { GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString() };
        GetDatasourceRequest request = new GetDatasourceRequest(names);
        assertNull(request.validate());

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        GetDatasourceRequest copiedRequest = new GetDatasourceRequest(input);

        // Verify
        assertArrayEquals(request.getNames(), copiedRequest.getNames());
    }
}
