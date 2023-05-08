/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.jobscheduler;

import org.opensearch.jobscheduler.spi.JobSchedulerExtension;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;

/**
 * Datasource job scheduler extension
 *
 * This extension is responsible for scheduling GeoIp data update task
 *
 * See https://github.com/opensearch-project/job-scheduler/blob/main/README.md#getting-started
 */
public class DatasourceExtension implements JobSchedulerExtension {
    /**
     * Job index name for a datasource
     */
    public static final String JOB_INDEX_NAME = ".scheduler_geospatial_ip2geo_datasource";

    @Override
    public String getJobType() {
        return "scheduler_geospatial_ip2geo_datasource";
    }

    @Override
    public String getJobIndex() {
        return JOB_INDEX_NAME;
    }

    @Override
    public ScheduledJobRunner getJobRunner() {
        return DatasourceRunner.getJobRunnerInstance();
    }

    @Override
    public ScheduledJobParser getJobParser() {
        return (parser, id, jobDocVersion) -> Datasource.PARSER.parse(parser, null);
    }
}
