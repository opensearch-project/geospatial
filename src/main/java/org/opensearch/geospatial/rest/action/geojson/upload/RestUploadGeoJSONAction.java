/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.rest.action.geojson.upload;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.action.geojson.upload.UploadGeoJSONAction;
import org.opensearch.geospatial.action.geojson.upload.UploadGeoJSONRequest;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

/**
 * Rest Action handler to accepts requests and route for geojson upload action
 */
public class RestUploadGeoJSONAction extends BaseRestHandler {

    public static final String ACTION_OBJECT = "geojson";
    public static final String ACTION_UPLOAD = "_upload";
    public static final String NAME = "upload_geojson_action";
    public static final String URL_DELIMITER = "/";

    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Supported Routes are
     * POST /_plugins/geospatial/geojson/_upload
     * {
     *     //TODO https://github.com/opensearch-project/geospatial/issues/29 (implement request body)
     * }
     *
     */
    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        return singletonList(new Route(POST, path));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        Tuple<XContentType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(sourceTuple.v2());
        return channel -> client.execute(UploadGeoJSONAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
