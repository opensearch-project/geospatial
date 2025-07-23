/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;

import inet.ipaddr.IPAddressString;
import lombok.extern.log4j.Log4j2;

/**
 * A class to check url against a deny-list
 */
@Log4j2
public class URLChecker {
    private static final int HTTP_REDIRECT_STATUS_MIN = 300;
    private static final int HTTP_REDIRECT_STATUS_MAX = 400;
    private static final String LOCATION_HEADER = "Location";
    private final ClusterSettings clusterSettings;

    public URLChecker(final ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    /**
     * Convert String to URL after verifying the url is not on a deny-list
     *
     * @param url value to validate and convert to URL
     * @return value in URL type
     */
    public URL toUrlIfAllowed(final String url) {
        try {
            return toUrlIfAllowed(url, clusterSettings.get(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST));
        } catch (UnknownHostException e) {
            log.error("Unknown host", e);
            throw new IllegalArgumentException("host provided in the datasource endpoint is unknown");
        } catch (MalformedURLException e) {
            log.error("Malformed URL", e);
            throw new IllegalArgumentException("URL provided in the datasource endpoint is malformed");
        }
    }

    @SuppressForbidden(reason = "Need to connect to http endpoint to read GeoIP database file")
    private URL toUrlIfAllowed(final String url, final List<String> denyList) throws UnknownHostException, MalformedURLException {
        URL urlToReturn = new URL(url);
        if (isInDenyList(new IPAddressString(InetAddress.getByName(urlToReturn.getHost()).getHostAddress()), denyList)) {
            throw new IllegalArgumentException(
                "given endpoint is blocked by deny list in cluster setting " + Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey()
            );
        }
        return urlToReturn;
    }

    private boolean isInDenyList(final IPAddressString url, final List<String> denyList) {
        return denyList.stream().map(cidr -> new IPAddressString(cidr)).anyMatch(cidr -> cidr.contains(url));
    }

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
