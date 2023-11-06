/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.util.Arrays;
import java.util.List;

import org.opensearch.common.settings.Settings;
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

    public void testDenyListDefaultValue() {
        List<String> privateNetworks = Arrays.asList(
            "127.0.0.0/8",
            "169.254.0.0/16",
            "10.0.0.0/8",
            "172.16.0.0/12",
            "192.168.0.0/16",
            "0.0.0.0/8",
            "100.64.0.0/10",
            "192.0.0.0/24",
            "192.0.2.0/24",
            "198.18.0.0/15",
            "192.88.99.0/24",
            "198.51.100.0/24",
            "203.0.113.0/24",
            "224.0.0.0/4",
            "240.0.0.0/4",
            "255.255.255.255/32",
            "::1/128",
            "fe80::/10",
            "fc00::/7",
            "::/128",
            "2001:db8::/32",
            "ff00::/8"
        );
        assertEquals(privateNetworks, Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.get(Settings.EMPTY));
    }
}
