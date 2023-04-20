/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;

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
        // TODO: This value is not correct. Update it later once CDN server is ready.
        "https://geoip.maps.opensearch.org/v1/geolite-2/manifest.json",
        new DatasourceEndpointValidator(),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Default update interval to be used in Ip2Geo datasource creation API
     */
    public static final Setting<TimeValue> DATASOURCE_UPDATE_INTERVAL = Setting.timeSetting(
        "plugins.geospatial.ip2geo.datasource.update_interval_in_days",
        TimeValue.timeValueDays(3),
        TimeValue.timeValueDays(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Timeout value for Ip2Geo processor
     */
    public static final Setting<TimeValue> TIMEOUT_IN_SECONDS = Setting.timeSetting(
        "plugins.geospatial.ip2geo.timeout_in_seconds",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Bulk size for indexing GeoIP data
     */
    public static final Setting<Integer> INDEXING_BULK_SIZE = Setting.intSetting(
        "plugins.geospatial.ip2geo.datasource.indexing_bulk_size",
        10000,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Cache size for GeoIP data
     */
    public static final Setting<Integer> CACHE_SIZE = Setting.intSetting(
        "plugins.geospatial.ip2geo.processor.cache_size",
        1000,
        0,
        Setting.Property.NodeScope
    );

    /**
     * Multi search bundle size for GeoIP data
     *
     * Multi search is used only when a field contains a list of ip addresses.
     */
    public static final Setting<Integer> MAX_BUNDLE_SIZE = Setting.intSetting(
        "plugins.geospatial.ip2geo.processor.max_bundle_size",
        100,
        1,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Multi search max concurrent searches
     *
     * Multi search is used only when a field contains a list of ip addresses.
     *
     * When the value is 0, it will use default value which will be decided
     * based on node count and search thread pool size.
     */
    public static final Setting<Integer> MAX_CONCURRENT_SEARCHES = Setting.intSetting(
        "plugins.geospatial.ip2geo.processor.max_concurrent_searches",
        0,
        0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Return all settings of Ip2Geo feature
     * @return a list of all settings for Ip2Geo feature
     */
    public static final List<Setting<?>> settings() {
        return List.of(
            DATASOURCE_ENDPOINT,
            DATASOURCE_UPDATE_INTERVAL,
            TIMEOUT_IN_SECONDS,
            INDEXING_BULK_SIZE,
            CACHE_SIZE,
            MAX_BUNDLE_SIZE,
            MAX_CONCURRENT_SEARCHES
        );
    }

    /**
     * Visible for testing
     */
    protected static class DatasourceEndpointValidator implements Setting.Validator<String> {
        @Override
        public void validate(final String value) {
            try {
                new URL(value).toURI();
            } catch (MalformedURLException | URISyntaxException e) {
                throw new IllegalArgumentException("Invalid URL format is provided");
            }
        }
    }
}
