/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Locale;

import lombok.extern.log4j.Log4j2;

/**
 * Utility class for validating HTTP connections no redirects
 */
@Log4j2
public class HttpRedirectValidator {
    private static final int HTTP_REDIRECT_STATUS_MIN = 300;
    private static final int HTTP_REDIRECT_STATUS_MAX = 400;
    private static final String LOCATION_HEADER = "Location";

    // Private constructor to prevent instantiation
    private HttpRedirectValidator() {}

    /**
     * Validates that an HTTP connection does not attempt to redirect
     *
     * @param httpConnection the HTTP connection to validate
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if a redirect attempt is detected
     */
    public static void validateNoRedirects(final HttpURLConnection httpConnection) throws IOException {
        httpConnection.setInstanceFollowRedirects(false);

        final int responseCode = httpConnection.getResponseCode();
        if (responseCode >= HTTP_REDIRECT_STATUS_MIN && responseCode < HTTP_REDIRECT_STATUS_MAX) {
            final String redirectLocation = httpConnection.getHeaderField(LOCATION_HEADER);
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "HTTP redirects are not allowed. URL [%s] attempted to redirect to [%s] with status code [%d]",
                    httpConnection.getURL().toString(),
                    redirectLocation != null ? redirectLocation : "unknown",
                    responseCode
                )
            );
        }
    }
}
