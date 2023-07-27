/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import lombok.SneakyThrows;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
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

    public void testValidate_whenNull_thenError() {
        DeleteDatasourceRequest request = new DeleteDatasourceRequest((String) null);

        // Run
        ActionRequestValidationException error = request.validate();

        // Verify
        assertNotNull(error.validationErrors());
        assertFalse(error.validationErrors().isEmpty());
    }

    public void testValidate_whenBlank_thenError() {
        DeleteDatasourceRequest request = new DeleteDatasourceRequest(" ");

        // Run
        ActionRequestValidationException error = request.validate();

        // Verify
        assertNotNull(error.validationErrors());
        assertFalse(error.validationErrors().isEmpty());
    }

    public void testValidate_whenInvalidDatasourceName_thenFails() {
        String invalidName = "_" + GeospatialTestHelper.randomLowerCaseString();
        DeleteDatasourceRequest request = new DeleteDatasourceRequest(invalidName);

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertTrue(exception.validationErrors().get(0).contains("no such datasource"));
    }
}
