/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.HttpURLConnection;

import org.opensearch.test.OpenSearchTestCase;

import lombok.SneakyThrows;

/**
 * Unit tests for HttpRedirectValidator
 */
public class HttpRedirectValidatorTests extends OpenSearchTestCase {

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
            HttpRedirectValidator.validateNoRedirects(connection);

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
                () -> HttpRedirectValidator.validateNoRedirects(connection)
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
