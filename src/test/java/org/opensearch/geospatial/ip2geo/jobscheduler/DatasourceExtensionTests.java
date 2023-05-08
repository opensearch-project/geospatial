/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import static org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension.JOB_INDEX_NAME;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

public class DatasourceExtensionTests extends Ip2GeoTestCase {
    public void testBasic() {
        DatasourceExtension extension = new DatasourceExtension();
        assertEquals("scheduler_geospatial_ip2geo_datasource", extension.getJobType());
        assertEquals(JOB_INDEX_NAME, extension.getJobIndex());
        assertEquals(DatasourceRunner.getJobRunnerInstance(), extension.getJobRunner());
    }

    public void testParser() throws Exception {
        DatasourceExtension extension = new DatasourceExtension();
        String id = GeospatialTestHelper.randomLowerCaseString();
        IntervalSchedule schedule = new IntervalSchedule(Instant.now().truncatedTo(ChronoUnit.MILLIS), 1, ChronoUnit.DAYS);
        String endpoint = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource(id, schedule, endpoint);

        Datasource anotherDatasource = (Datasource) extension.getJobParser()
            .parse(
                createParser(datasource.toXContent(XContentFactory.jsonBuilder(), null)),
                GeospatialTestHelper.randomLowerCaseString(),
                new JobDocVersion(randomPositiveLong(), randomPositiveLong(), randomPositiveLong())
            );

        assertTrue(datasource.equals(anotherDatasource));
    }
}
