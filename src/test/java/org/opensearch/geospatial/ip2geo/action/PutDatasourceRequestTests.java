/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.util.Locale;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class PutDatasourceRequestTests extends Ip2GeoTestCase {

    public void testValidateWithInvalidUrl() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint("invalidUrl");
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals("Invalid URL format is provided", exception.validationErrors().get(0));
    }

    public void testValidateWithInvalidManifestFile() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String domain = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(String.format(Locale.ROOT, "https://%s.com", domain));
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals(
            String.format(Locale.ROOT, "Error occurred while reading a file from %s", request.getEndpoint()),
            exception.validationErrors().get(0)
        );
    }

    public void testValidate() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));
        assertNull(request.validate());
    }

    public void testValidateWithZeroUpdateInterval() throws Exception {
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

    public void testValidateWithLargeUpdateInterval() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(30));

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertTrue(exception.validationErrors().get(0).contains("should be smaller"));
    }

    public void testValidateWithInvalidUrlInsideManifest() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(sampleManifestUrlWithInvalidUrl());
        request.setUpdateInterval(TimeValue.timeValueDays(1));

        // Run
        ActionRequestValidationException exception = request.validate();

        // Verify
        assertEquals(1, exception.validationErrors().size());
        assertTrue(exception.validationErrors().get(0).contains("Invalid URL format"));
    }

    public void testStreamInOut() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String domain = GeospatialTestHelper.randomLowerCaseString();
        PutDatasourceRequest request = new PutDatasourceRequest(datasourceName);
        request.setEndpoint(String.format(Locale.ROOT, "https://%s.com", domain));
        request.setUpdateInterval(TimeValue.timeValueDays(Randomness.get().nextInt(30) + 1));

        // Run
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        PutDatasourceRequest copiedRequest = new PutDatasourceRequest(input);

        // Verify
        assertEquals(request, copiedRequest);
    }
}
