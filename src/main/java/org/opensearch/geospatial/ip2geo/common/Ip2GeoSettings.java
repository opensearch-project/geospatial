/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;

/**
 * Settings for Ip2Geo datasource operations
 */
public class Ip2GeoSettings {

    /**
     * Default endpoint to be used in GeoIP datasource creation API
     */
    public static final Setting<String> DATASOURCE_ENDPOINT = Setting.simpleString(
        "plugins.geospatial.ip2geo.datasource.endpoint",
        "https://geoip.maps.opensearch.org/v1/geolite2-city/manifest.json",
        new DatasourceEndpointValidator(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default update interval to be used in Ip2Geo datasource creation API
     */
    public static final Setting<Long> DATASOURCE_UPDATE_INTERVAL = Setting.longSetting(
        "plugins.geospatial.ip2geo.datasource.update_interval_in_days",
        3l,
        1l,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Bulk size for indexing GeoIP data
     */
    public static final Setting<Integer> BATCH_SIZE = Setting.intSetting(
        "plugins.geospatial.ip2geo.datasource.batch_size",
        10000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Timeout value for Ip2Geo processor
     */
    public static final Setting<TimeValue> TIMEOUT = Setting.timeSetting(
        "plugins.geospatial.ip2geo.timeout",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Max size for geo data cache
     */
    public static final Setting<Long> CACHE_SIZE = Setting.longSetting(
        "plugins.geospatial.ip2geo.processor.cache_size",
        1000,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * A list of CIDR which will be blocked to be used as datasource endpoint
     * Private network addresses will be blocked as default
     */
    public static final Setting<List<String>> DATASOURCE_ENDPOINT_DENYLIST = Setting.listSetting(
        "plugins.geospatial.ip2geo.datasource.endpoint.denylist",
        Arrays.asList(
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
        ),
        Function.identity(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Return all settings of Ip2Geo feature
     * @return a list of all settings for Ip2Geo feature
     */
    public static final List<Setting<?>> settings() {
        return List.of(DATASOURCE_ENDPOINT, DATASOURCE_UPDATE_INTERVAL, BATCH_SIZE, TIMEOUT, CACHE_SIZE, DATASOURCE_ENDPOINT_DENYLIST);
    }

    /**
     * Visible for testing
     */
    protected static class DatasourceEndpointValidator implements Setting.Validator<String> {
        @Override
        public void validate(final String value) {
            try {
                URI.create(value).toURL().toURI();
            } catch (MalformedURLException | URISyntaxException | IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid URL format is provided");
            }
        }
    }
}
