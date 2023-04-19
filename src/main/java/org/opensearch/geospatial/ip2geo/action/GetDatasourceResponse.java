/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;

/**
 * GeoIP datasource get request
 */
@Getter
@Setter
@Log4j2
public class GetDatasourceResponse extends ActionResponse implements ToXContentObject {
    private final String FIELD_NAME_DATASOURCES = "datasources";
    private final String FIELD_NAME_NAME = "name";
    private final String FIELD_NAME_STATE = "state";
    private final String FIELD_NAME_ENDPOINT = "endpoint";
    private final String FIELD_NAME_UPDATE_INTERVAL = "update_interval_in_days";
    private final String FIELD_NAME_NEXT_UPDATE_AT = "next_update_at_in_epoch_millis";
    private final String FIELD_NAME_NEXT_UPDATE_AT_READABLE = "next_update_at";
    private final String FIELD_NAME_DATABASE = "database";
    private final String FIELD_NAME_UPDATE_STATS = "update_stats";
    private List<Datasource> datasources;

    /**
     * Default constructor
     *
     * @param datasources List of datasources
     */
    public GetDatasourceResponse(final List<Datasource> datasources) {
        this.datasources = datasources;
    }

    /**
     * Constructor with StreamInput
     *
     * @param in the stream input
     */
    public GetDatasourceResponse(final StreamInput in) throws IOException {
        in.readList(Datasource::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeList(datasources);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.startArray(FIELD_NAME_DATASOURCES);
        for (Datasource datasource : datasources) {
            builder.startObject();
            builder.field(FIELD_NAME_NAME, datasource.getName());
            builder.field(FIELD_NAME_STATE, datasource.getState());
            builder.field(FIELD_NAME_ENDPOINT, datasource.getEndpoint());
            builder.field(FIELD_NAME_UPDATE_INTERVAL, datasource.getSchedule().getInterval());
            builder.timeField(
                FIELD_NAME_NEXT_UPDATE_AT,
                FIELD_NAME_NEXT_UPDATE_AT_READABLE,
                datasource.getSchedule().getNextExecutionTime(Instant.now()).toEpochMilli()
            );
            builder.field(FIELD_NAME_DATABASE, datasource.getDatabase());
            builder.field(FIELD_NAME_UPDATE_STATS, datasource.getUpdateStats());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
