/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.util.Locale;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class PutDatasourceRequestTests extends Ip2GeoTestCase {

    public void testValidate_whenInvalidUrl_thenFails() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint("invalidUrl");
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals("Invalid URL format is provided", exception.validationErrors().get(0));
    }

    public void testValidate_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        assertNull(request.validate());
    }

    public void testValidate_whenInvalidDatasourceName_thenFails() {
        String invalidName = "_" + GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(invalidName);
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertTrue(exception.validationErrors().get(0).contains("must not"));
    }

    public void testValidate_whenZeroUpdateInterval_thenFails() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(0));

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertEquals(
            String.format(Locale.ROOT, "Update interval should be equal to or larger than 1 day"),
            exception.validationErrors().get(0)
        );
    }

    public void testStreamInOut_whenValidInput_thenSucceed() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String domain = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(String.format(Locale.ROOT, "https://%s.com", domain));
        request.setUpdateInterval(TimeValue.timeValueDays(Randomness.get().nextInt(29) + 1));

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        PutDatasourceRequest copiedRequest = new PutDatasourceRequest(input);

        // Verify
        assertEquals(request.getName(), copiedRequest.getName());
        assertEquals(request.getUpdateInterval(), copiedRequest.getUpdateInterval());
        assertEquals(request.getEndpoint(), copiedRequest.getEndpoint());
    }
}
