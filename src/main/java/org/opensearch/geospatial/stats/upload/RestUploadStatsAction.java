/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import static org.opensearch.geospatial.shared.URLBuilder.URL_DELIMITER;
import static org.opensearch.geospatial.shared.URLBuilder.getPluginURLPrefix;
import static org.opensearch.rest.RestRequest.Method.GET;

import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

public class RestUploadStatsAction extends BaseRestHandler {

    private static final String NAME = "upload_stats";
    public static final String ACTION_OBJECT = "stats";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, getPluginURLPrefix(), ACTION_OBJECT);
        return List.of(new Route(GET, path));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
        return channel -> nodeClient.execute(UploadStatsAction.INSTANCE, new UploadStatsRequest(), new RestToXContentListener<>(channel));
    }
}
