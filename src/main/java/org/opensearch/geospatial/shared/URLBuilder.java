/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.shared;

/**
 * Helper to build url path for this plugin
 */
public final class URLBuilder {

    public static final String NAME = "geospatial";
    public static final String PLUGIN_PREFIX = "_plugins";
    public static final String URL_DELIMITER = "/";

    // Avoid instantiation of this class
    private URLBuilder() {}

    /**
     * @return plugin URL prefix path for {@link org.opensearch.geospatial.plugin.GeospatialPlugin}
     */
    public static String getPluginURLPrefix() {
        return String.join(URL_DELIMITER, PLUGIN_PREFIX, NAME);
    }
}
