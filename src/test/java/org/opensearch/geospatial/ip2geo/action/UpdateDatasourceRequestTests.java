/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.util.Locale;

import lombok.SneakyThrows;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class UpdateDatasourceRequestTests extends Ip2GeoTestCase {

    public void testValidate_whenNullValues_thenFails() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertEquals("no values to update", exception.validationErrors().get(0));
    }

    public void testValidate_whenInvalidUrl_thenFails() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);
        request.setEndpoint("invalidUrl");

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertEquals("Invalid URL format is provided", exception.validationErrors().get(0));
    }

    public void testValidate_whenInvalidManifestFile_thenFails() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String domain = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);
        request.setEndpoint(String.format(Locale.ROOT, "https://%s.com", domain));

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertTrue(exception.validationErrors().get(0).contains("Error occurred while reading a file"));
    }

    @SneakyThrows
    public void testValidate_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        // Run and verify
        assertNull(request.validate());
    }

    @SneakyThrows
    public void testValidate_whenZeroUpdateInterval_thenFails() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);
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

    @SneakyThrows
    public void testValidate_whenInvalidUrlInsideManifest_thenFail() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrlWithInvalidUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertTrue(exception.validationErrors().get(0).contains("Invalid URL format"));
    }

    @SneakyThrows
    public void testStreamInOut_whenValidInput_thenSucceed() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String domain = GeospatialTestHelper.randomLowerCaseString();
        UpdateDatasourceRequest request = new UpdateDatasourceRequest(datasourceName);
        request.setEndpoint(String.format(Locale.ROOT, "https://%s.com", domain));
        request.setUpdateInterval(TimeValue.timeValueDays(Randomness.get().nextInt(29) + 1));

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        UpdateDatasourceRequest copiedRequest = new UpdateDatasourceRequest(input);

        // Verify
        assertEquals(request, copiedRequest);
    }
}
