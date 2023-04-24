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
import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Locale;

import org.opensearch.common.Randomness;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.OpenSearchTestCase;

public class DatasourceTests extends OpenSearchTestCase {
    public void testCurrentIndexName() {
        String id = GeospatialTestHelper.randomLowerCaseString();
        Instant now = Instant.now();
        Datasource datasource = new Datasource();
        datasource.setId(id);
        datasource.getDatabase().setProvider("provider");
        datasource.getDatabase().setMd5Hash("md5Hash");
        datasource.getDatabase().setUpdatedAt(now);
        datasource.getDatabase().setValidForInDays(10l);
        datasource.getDatabase().setFields(new ArrayList<>());
        assertEquals(
            String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATA_INDEX_NAME_PREFIX, id, now.toEpochMilli()),
            datasource.currentIndexName()
        );
    }

    public void testGetIndexNameFor() {
        long updatedAt = Randomness.get().nextLong();
        DatasourceManifest manifest = mock(DatasourceManifest.class);
        when(manifest.getUpdatedAt()).thenReturn(updatedAt);

        String id = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource();
        datasource.setId(id);
        assertEquals(
            String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATA_INDEX_NAME_PREFIX, id, updatedAt),
            datasource.indexNameFor(manifest)
        );
    }

    public void testGetJitter() {
        Datasource datasource = new Datasource();
        datasource.setSchedule(new IntervalSchedule(Instant.now(), Randomness.get().nextInt(31), ChronoUnit.DAYS));
        long intervalInMinutes = datasource.getSchedule().getInterval() * 60 * 24;
        double sixMinutes = 6;
        assertTrue(datasource.getJitter() * intervalInMinutes <= sixMinutes);
    }
}
