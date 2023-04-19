/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.Client;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.ip2geo.common.DatasourceHelper;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action to get datasource
 */
@Log4j2
public class GetDatasourceTransportAction extends HandledTransportAction<GetDatasourceRequest, GetDatasourceResponse> {
    private final Client client;

    /**
     * Default constructor
     * @param transportService the transport service
     * @param actionFilters the action filters
     * @param client the client
     */
    @Inject
    public GetDatasourceTransportAction(final TransportService transportService, final ActionFilters actionFilters, final Client client) {
        super(GetDatasourceAction.NAME, transportService, actionFilters, GetDatasourceRequest::new);
        this.client = client;
    }

    @Override
    protected void doExecute(final Task task, final GetDatasourceRequest request, final ActionListener<GetDatasourceResponse> listener) {
        if (request.getNames().length == 0 || (request.getNames().length == 1 && "_all".equals(request.getNames()[0]))) {
            // Don't expect too many data sources. Therefore, querying all data sources without pagination should be fine.
            DatasourceHelper.getAllDatasources(client, new ActionListener<>() {
                @Override
                public void onResponse(final List<Datasource> datasources) {
                    listener.onResponse(new GetDatasourceResponse(datasources));
                }

                @Override
                public void onFailure(final Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            DatasourceHelper.getDatasources(client, request.getNames(), new ActionListener<>() {
                @Override
                public void onResponse(final List<Datasource> datasources) {
                    listener.onResponse(new GetDatasourceResponse(datasources));
                }

                @Override
                public void onFailure(final Exception e) {
                    listener.onFailure(e);
                }
            });
        }
    }
}
