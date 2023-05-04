/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

/**
 * Rest handler for Ip2Geo datasource creation
 *
 * This handler handles a request of
 * PUT /_plugins/geospatial/ip2geo/datasource/{id}
 * {
 *     "endpoint": {endpoint},
 *     "update_interval_in_days": 3
 * }
 *
 * When request is received, it will create a datasource by downloading GeoIp data from the endpoint.
 * After the creation of datasource is completed, it will schedule the next update task after update_interval_in_days.
 *
 */
public class RestPutDatasourceHandler extends BaseRestHandler {
    private static final String ACTION_NAME = "ip2geo_datasource_put";
    private final ClusterSettings clusterSettings;

    public RestPutDatasourceHandler(final ClusterSettings clusterSettings) {
        this.clusterSettings = clusterSettings;
    }

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final PutDatasourceRequest putDatasourceRequest = new PutDatasourceRequest(request.param("name"));
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                PutDatasourceRequest.PARSER.parse(parser, putDatasourceRequest, null);
            }
        }
        if (putDatasourceRequest.getEndpoint() == null) {
            putDatasourceRequest.setEndpoint(clusterSettings.get(Ip2GeoSettings.DATASOURCE_ENDPOINT));
        }
        if (putDatasourceRequest.getUpdateInterval() == null) {
            putDatasourceRequest.setUpdateInterval(TimeValue.timeValueDays(clusterSettings.get(Ip2GeoSettings.DATASOURCE_UPDATE_INTERVAL)));
        }
        return channel -> client.executeLocally(PutDatasourceAction.INSTANCE, putDatasourceRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/{name}");
        return List.of(new Route(PUT, path));
    }
}
