/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;

import inet.ipaddr.IPAddressString;

/**
 * A class to check url against a deny-list
 */
@Log4j2
public class URLDenyListChecker {
    private final ClusterSettings clusterSettings;

    public URLDenyListChecker(final ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    /**
     * Convert String to URL after verifying the url is not on a deny-list
     *
     * @param url value to validate and convert to URL
     * @return value in URL type
     */
    public URL toUrlIfNotInDenyList(final String url) {
        try {
            return toUrlIfNotInDenyList(url, clusterSettings.get(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST));
        } catch (UnknownHostException e) {
            log.error("Unknown host", e);
            throw new IllegalArgumentException("host provided in the datasource endpoint is unknown");
        } catch (MalformedURLException e) {
            log.error("Malformed URL", e);
            throw new IllegalArgumentException("URL provided in the datasource endpoint is malformed");
        }
    }

    @SuppressForbidden(reason = "Need to connect to http endpoint to read GeoIP database file")
    private URL toUrlIfNotInDenyList(final String url, final List<String> denyList) throws UnknownHostException, MalformedURLException {
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
}
