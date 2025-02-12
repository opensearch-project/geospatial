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

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geospatial.ip2geo.common.URLDenyListChecker;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Rest handler for Ip2Geo datasource update request
 */
public class RestUpdateDatasourceHandler extends BaseRestHandler {
    private static final String ACTION_NAME = "ip2geo_datasource_update";

    private final URLDenyListChecker urlDenyListChecker;

    public RestUpdateDatasourceHandler(final URLDenyListChecker urlDenyListChecker) {
        this.urlDenyListChecker = urlDenyListChecker;
    }

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final UpdateDatasourceRequest updateDatasourceRequest = new UpdateDatasourceRequest(request.param("name"));
        if (request.hasContentOrSourceParam()) {
            try (XContentParser parser = request.contentOrSourceParamParser()) {
                UpdateDatasourceRequest.PARSER.parse(parser, updateDatasourceRequest, null);
            }
        }
        if (updateDatasourceRequest.getEndpoint() != null) {
            // Call to validate if URL is in a deny-list or not.
            urlDenyListChecker.toUrlIfNotInDenyList(updateDatasourceRequest.getEndpoint());
        }
        return channel -> client.executeLocally(
            UpdateDatasourceAction.INSTANCE,
            updateDatasourceRequest,
            new RestToXContentListener<>(channel)
        );
    }

    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/{name}/_settings");
        return List.of(new Route(PUT, path));
    }
}
