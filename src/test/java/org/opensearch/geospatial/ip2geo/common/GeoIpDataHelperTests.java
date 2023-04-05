/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import org.opensearch.test.OpenSearchTestCase;

public class GeoIpDataHelperTests extends OpenSearchTestCase {
    public void testCreateDocument() {
        String[] names = { "ip", "country", "city" };
        String[] values = { "1.0.0.0/25", "USA", "Seattle" };
        assertEquals(
            "{\"_cidr\":\"1.0.0.0/25\",\"_data\":{\"country\":\"USA\",\"city\":\"Seattle\"}}",
            GeoIpDataHelper.createDocument(names, values)
        );
    }
}
