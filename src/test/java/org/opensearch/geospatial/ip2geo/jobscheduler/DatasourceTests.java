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
import java.util.Arrays;
import java.util.Locale;

import org.opensearch.common.Randomness;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.OpenSearchTestCase;

public class DatasourceTests extends OpenSearchTestCase {

    public void testParser() throws Exception {
        String id = GeospatialTestHelper.randomLowerCaseString();
        IntervalSchedule schedule = new IntervalSchedule(Instant.now().truncatedTo(ChronoUnit.MILLIS), 1, ChronoUnit.DAYS);
        String endpoint = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource(id, schedule, endpoint);
        datasource.enable();
        datasource.getDatabase().setFields(Arrays.asList("field1", "field2"));
        datasource.getDatabase().setProvider("test_provider");
        datasource.getDatabase().setUpdatedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        datasource.getDatabase().setMd5Hash(GeospatialTestHelper.randomLowerCaseString());
        datasource.getDatabase().setValidForInDays(1l);
        datasource.getUpdateStats().setLastProcessingTimeInMillis(Randomness.get().nextLong());
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        datasource.getUpdateStats().setLastSkippedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        datasource.getUpdateStats().setLastFailedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));

        Datasource anotherDatasource = Datasource.PARSER.parse(
            createParser(datasource.toXContent(XContentFactory.jsonBuilder(), null)),
            null
        );
        assertTrue(datasource.equals(anotherDatasource));
    }

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
        datasource.setSchedule(new IntervalSchedule(Instant.now(), Randomness.get().ints(1, 31).findFirst().getAsInt(), ChronoUnit.DAYS));
        long intervalInMinutes = datasource.getSchedule().getInterval() * 60l * 24l;
        double sixMinutes = 6;
        assertTrue(datasource.getJitter() * intervalInMinutes <= sixMinutes);
    }

    public void testIsExpired() {
        Datasource datasource = new Datasource();
        // never expire if validForInDays is null
        assertFalse(datasource.isExpired());

        datasource.getDatabase().setValidForInDays(1l);

        // if last skipped date is null, use only last succeeded date to determine
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(25, ChronoUnit.HOURS));
        assertTrue(datasource.isExpired());

        // use the latest date between last skipped date and last succeeded date to determine
        datasource.getUpdateStats().setLastSkippedAt(Instant.now());
        assertFalse(datasource.isExpired());
        datasource.getUpdateStats().setLastSkippedAt(Instant.now().minus(25, ChronoUnit.HOURS));
        datasource.getUpdateStats().setLastSucceededAt(Instant.now());
        assertFalse(datasource.isExpired());
    }

    public void testLockDurationSeconds() {
        Datasource datasource = new Datasource();
        assertNotNull(datasource.getLockDurationSeconds());
    }
}
