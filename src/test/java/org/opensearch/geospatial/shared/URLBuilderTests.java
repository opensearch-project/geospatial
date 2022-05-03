/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.shared;

import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;

import org.opensearch.test.OpenSearchTestCase;

public class URLBuilderTests extends OpenSearchTestCase {

    public void testPluginPrefix() {
        String pluginPrefix = getPluginURLPrefix();
        assertEquals("_plugins/geospatial", pluginPrefix);
    }
}
