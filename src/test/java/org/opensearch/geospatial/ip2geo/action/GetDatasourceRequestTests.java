/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
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

    public void testValidate_whenNull_thenError() {
        GetDatasourceRequest request = new GetDatasourceRequest((String[]) null);

        // Run
        ActionRequestValidationException error = request.validate();

        // Verify
        assertNotNull(error.validationErrors());
        assertFalse(error.validationErrors().isEmpty());
    }
}
