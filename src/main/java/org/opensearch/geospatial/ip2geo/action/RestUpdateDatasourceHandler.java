/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

/**
 * Rest handler for Ip2Geo datasource update request
 */
public class RestUpdateDatasourceHandler extends BaseRestHandler {
    private static final String ACTION_NAME = "ip2geo_datasource_update";

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
        return channel -> client.executeLocally(
            UpdateDatasourceAction.INSTANCE,
            putDatasourceRequest,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/{name}/_settings");
        return List.of(new Route(PUT, path));
    }
}
