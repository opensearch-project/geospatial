/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.plugin.GeospatialPlugin.IP2GEO_DATASOURCE_INDEX_NAME_PREFIX;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Locale;

import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.test.OpenSearchTestCase;

public class DatasourceTests extends OpenSearchTestCase {
    public void testCurrentIndexName() {
        String id = "test";
        Instant now = Instant.now();
        Datasource datasource = new Datasource();
        datasource.setName(id);
        datasource.setDatabase(new Datasource.Database("provider", "md5Hash", now, 10l, new ArrayList<>()));
        assertEquals(
            String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATASOURCE_INDEX_NAME_PREFIX, id, now.toEpochMilli()),
            datasource.currentIndexName()
        );
    }

    public void testGetIndexNameFor() {
        long updatedAt = 123123123l;
        DatasourceManifest manifest = mock(DatasourceManifest.class);
        when(manifest.getUpdatedAt()).thenReturn(updatedAt);

        String id = "test";
        Datasource datasource = new Datasource();
        datasource.setName(id);
        assertEquals(
            String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATASOURCE_INDEX_NAME_PREFIX, id, updatedAt),
            datasource.indexNameFor(manifest)
        );
    }
}
