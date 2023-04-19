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
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.ArrayList;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

/**
 * Rest handler for Ip2Geo datasource get request
 */
public class RestGetDatasourceAction extends BaseRestHandler {
    private static final String ACTION_NAME = "ip2geo_datasource_get";

    @Override
    public String getName() {
        return ACTION_NAME;
    }

    @Override
    protected RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final String[] names = request.paramAsStringArray("name", Strings.EMPTY_ARRAY);
        final GetDatasourceRequest getDatasourceRequest = new GetDatasourceRequest(names);

        return channel -> client.executeLocally(GetDatasourceAction.INSTANCE, getDatasourceRequest, new RestToXContentListener<>(channel));
    }

    @Override
    public List<Route> routes() {
        boolean enabled = true;
        String path1 = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource");
        String path2 = String.join(URL_DELIMITER, getPluginURLPrefix(), "ip2geo/datasource/{name}");
        return enabled ? List.of(new Route(GET, path1), new Route(GET, path2)) : new ArrayList<>();
    }
}
