/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import org.opensearch.test.OpenSearchTestCase;

public class Ip2GeoSettingsTests extends OpenSearchTestCase {
    public void testValidateInvalidUrl() {
        Ip2GeoSettings.DatasourceEndpointValidator validator = new Ip2GeoSettings.DatasourceEndpointValidator();
        Exception e = expectThrows(IllegalArgumentException.class, () -> validator.validate("InvalidUrl"));
        assertEquals("Invalid URL format is provided", e.getMessage());
    }

    public void testValidateValidUrl() {
        Ip2GeoSettings.DatasourceEndpointValidator validator = new Ip2GeoSettings.DatasourceEndpointValidator();
        validator.validate("https://test.com");
    }
}
