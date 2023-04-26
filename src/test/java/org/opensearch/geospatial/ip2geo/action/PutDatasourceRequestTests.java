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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

public class PutDatasourceRequestTests extends OpenSearchTestCase {

    public void testValidateInvalidUrl() {
        PutDatasourceRequest request = new PutDatasourceRequest("test");
        request.setEndpoint("invalidUrl");
        request.setUpdateIntervalInDays(TimeValue.ZERO);
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals("Invalid URL format is provided", exception.validationErrors().get(0));
    }

    public void testValidateInvalidManifestFile() {
        PutDatasourceRequest request = new PutDatasourceRequest("test");
        request.setDatasourceName("test");
        request.setEndpoint("https://hi.com");
        request.setUpdateIntervalInDays(TimeValue.ZERO);
        ActionRequestValidationException exception = request.validate();
        assertEquals(1, exception.validationErrors().size());
        assertEquals(
            String.format(Locale.ROOT, "Error occurred while reading a file from %s", request.getEndpoint()),
            exception.validationErrors().get(0)
        );
    }
}
