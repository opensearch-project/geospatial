/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
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

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.common.DatasourceManifest;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

public class DatasourceTests extends Ip2GeoTestCase {

    public void testParser() throws Exception {
        String id = GeospatialTestHelper.randomLowerCaseString();
        IntervalSchedule schedule = new IntervalSchedule(Instant.now().truncatedTo(ChronoUnit.MILLIS), 1, ChronoUnit.DAYS);
        String endpoint = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource(id, schedule, endpoint);
        datasource.enable();
        datasource.getDatabase().setFields(Arrays.asList("field1", "field2"));
        datasource.getDatabase().setProvider("test_provider");
        datasource.getDatabase().setUpdatedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        datasource.getDatabase().setSha256Hash(GeospatialTestHelper.randomLowerCaseString());
        datasource.getDatabase().setValidForInDays(1l);
        datasource.getUpdateStats().setLastProcessingTimeInMillis(randomPositiveLong());
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        datasource.getUpdateStats().setLastSkippedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));
        datasource.getUpdateStats().setLastFailedAt(Instant.now().truncatedTo(ChronoUnit.MILLIS));

        Datasource anotherDatasource = Datasource.PARSER.parse(
            createParser(datasource.toXContent(XContentFactory.jsonBuilder(), null)),
            null
        );
        assertTrue(datasource.equals(anotherDatasource));
    }

    public void testCurrentIndexName_whenNotExpired_thenReturnName() {
        String id = GeospatialTestHelper.randomLowerCaseString();
        Instant now = Instant.now();
        Datasource datasource = new Datasource();
        datasource.setName(id);
        datasource.getDatabase().setProvider("provider");
        datasource.getDatabase().setSha256Hash("sha256Hash");
        datasource.getDatabase().setUpdatedAt(now);
        datasource.getDatabase().setFields(new ArrayList<>());

        assertEquals(
            String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATA_INDEX_NAME_PREFIX, id, now.toEpochMilli()),
            datasource.currentIndexName()
        );
    }

    public void testCurrentIndexName_whenExpired_thenReturnNull() {
        String id = GeospatialTestHelper.randomLowerCaseString();
        Instant now = Instant.now();
        Datasource datasource = new Datasource();
        datasource.setName(id);
        datasource.getDatabase().setProvider("provider");
        datasource.getDatabase().setSha256Hash("sha256Hash");
        datasource.getDatabase().setUpdatedAt(now);
        datasource.getDatabase().setValidForInDays(1l);
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(25, ChronoUnit.HOURS));
        datasource.getDatabase().setFields(new ArrayList<>());

        assertTrue(datasource.isExpired());
        assertNull(datasource.currentIndexName());
    }

    public void testGetIndexNameFor() {
        long updatedAt = randomPositiveLong();
        DatasourceManifest manifest = mock(DatasourceManifest.class);
        when(manifest.getUpdatedAt()).thenReturn(updatedAt);

        String id = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource();
        datasource.setName(id);
        assertEquals(
            String.format(Locale.ROOT, "%s.%s.%d", IP2GEO_DATA_INDEX_NAME_PREFIX, id, updatedAt),
            datasource.indexNameFor(manifest)
        );
    }

    public void testIsExpired_whenCalled_thenExpectedValue() {
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

    public void testWillExpired_whenCalled_thenExpectedValue() {
        Datasource datasource = new Datasource();
        // never expire if validForInDays is null
        assertFalse(datasource.willExpire(Instant.MAX));

        long validForInDays = 1;
        datasource.getDatabase().setValidForInDays(validForInDays);

        // if last skipped date is null, use only last succeeded date to determine
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(1, ChronoUnit.DAYS));
        assertTrue(
            datasource.willExpire(datasource.getUpdateStats().getLastSucceededAt().plus(validForInDays, ChronoUnit.DAYS).plusSeconds(1))
        );
        assertFalse(datasource.willExpire(datasource.getUpdateStats().getLastSucceededAt().plus(validForInDays, ChronoUnit.DAYS)));

        // use the latest date between last skipped date and last succeeded date to determine
        datasource.getUpdateStats().setLastSkippedAt(Instant.now());
        assertTrue(
            datasource.willExpire(datasource.getUpdateStats().getLastSkippedAt().plus(validForInDays, ChronoUnit.DAYS).plusSeconds(1))
        );
        assertFalse(datasource.willExpire(datasource.getUpdateStats().getLastSkippedAt().plus(validForInDays, ChronoUnit.DAYS)));

        datasource.getUpdateStats().setLastSkippedAt(Instant.now().minus(1, ChronoUnit.HOURS));
        datasource.getUpdateStats().setLastSucceededAt(Instant.now());
        assertTrue(
            datasource.willExpire(datasource.getUpdateStats().getLastSucceededAt().plus(validForInDays, ChronoUnit.DAYS).plusSeconds(1))
        );
        assertFalse(datasource.willExpire(datasource.getUpdateStats().getLastSucceededAt().plus(validForInDays, ChronoUnit.DAYS)));
    }

    public void testExpirationDay_whenCalled_thenExpectedValue() {
        Datasource datasource = new Datasource();
        datasource.getDatabase().setValidForInDays(null);
        assertEquals(Instant.MAX, datasource.expirationDay());

        long validForInDays = 1;
        datasource.getDatabase().setValidForInDays(validForInDays);

        // if last skipped date is null, use only last succeeded date to determine
        datasource.getUpdateStats().setLastSucceededAt(Instant.now().minus(1, ChronoUnit.DAYS));
        assertEquals(datasource.getUpdateStats().getLastSucceededAt().plus(validForInDays, ChronoUnit.DAYS), datasource.expirationDay());

        // use the latest date between last skipped date and last succeeded date to determine
        datasource.getUpdateStats().setLastSkippedAt(Instant.now());
        assertEquals(datasource.getUpdateStats().getLastSkippedAt().plus(validForInDays, ChronoUnit.DAYS), datasource.expirationDay());

        datasource.getUpdateStats().setLastSkippedAt(Instant.now().minus(1, ChronoUnit.HOURS));
        datasource.getUpdateStats().setLastSucceededAt(Instant.now());
        assertEquals(datasource.getUpdateStats().getLastSucceededAt().plus(validForInDays, ChronoUnit.DAYS), datasource.expirationDay());
    }

    public void testLockDurationSeconds() {
        Datasource datasource = new Datasource();
        assertNotNull(datasource.getLockDurationSeconds());
    }
}
