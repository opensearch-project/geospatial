/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.junit.Before;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.geospatial.ClusterSettingHelper;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

public class URLCheckerTests extends OpenSearchTestCase {
    private ClusterSettingHelper clusterSettingHelper;

    @Before
    public void init() {
        clusterSettingHelper = new ClusterSettingHelper();
    }

    @SneakyThrows
    public void testToUrlIfNotInDenyListWithBlockedAddress() {
        Node mockNode = clusterSettingHelper.createMockNode(
            Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Arrays.asList("127.0.0.0/8"))
        );
        mockNode.start();
        try {
            ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
            URLChecker urlDenyListChecker = new URLChecker(clusterService.getClusterSettings());
            String endpoint = String.format(
                Locale.ROOT,
                "https://127.%d.%d.%d/v1/manifest.json",
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256)
            );
            expectThrows(IllegalArgumentException.class, () -> urlDenyListChecker.toUrlIfAllowed(endpoint));
        } finally {
            mockNode.close();
        }
    }

    @SneakyThrows
    public void testToUrlIfNotInDenyListWithNonBlockedAddress() {
        Node mockNode = clusterSettingHelper.createMockNode(
            Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Arrays.asList("127.0.0.0/8"))
        );
        mockNode.start();
        try {
            ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
            URLChecker urlDenyListChecker = new URLChecker(clusterService.getClusterSettings());
            String endpoint = String.format(
                Locale.ROOT,
                "https://128.%d.%d.%d/v1/manifest.json",
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256)
            );
            // Expect no exception
            urlDenyListChecker.toUrlIfAllowed(endpoint);
        } finally {
            mockNode.close();
        }
    }

    /**
     * Test that non-redirect status codes (200, 404, etc.) are allowed
     */
    @SneakyThrows
    public void testValidateNoRedirects_whenNonRedirectStatus_thenAllowed() {
        int[] nonRedirectCodes = { 200, 201, 400, 404, 500 };

        for (int statusCode : nonRedirectCodes) {
            HttpURLConnection connection = mock(HttpURLConnection.class);
            when(connection.getResponseCode()).thenReturn(statusCode);

            // Should not throw any exception
            URLChecker.validateNoRedirects(connection);

            // Verify that redirects are disabled
            verify(connection).setInstanceFollowRedirects(false);
        }
    }

    /**
     * Test that redirect status codes (3xx) are blocked
     */
    @SneakyThrows
    public void testValidateNoRedirects_whenRedirectStatus_thenBlocked() {
        int[] redirectCodes = { 300, 301, 302, 303, 307, 308 };

        for (int statusCode : redirectCodes) {
            HttpURLConnection connection = mock(HttpURLConnection.class);
            when(connection.getResponseCode()).thenReturn(statusCode);
            when(connection.getHeaderField("Location")).thenReturn("http://redirect-target.com");
            when(connection.getURL()).thenReturn(new java.net.URL("https://example.com/test"));

            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> URLChecker.validateNoRedirects(connection)
            );

            // Verify error message
            assertTrue(
                "Error message should mention redirects are not allowed",
                exception.getMessage().contains("HTTP redirects are not allowed")
            );
            assertTrue("Error message should contain status code", exception.getMessage().contains(String.valueOf(statusCode)));
            assertTrue("Error message should contain original URL", exception.getMessage().contains("https://example.com/test"));
            assertTrue("Error message should contain redirect target", exception.getMessage().contains("http://redirect-target.com"));
        }
    }
}
