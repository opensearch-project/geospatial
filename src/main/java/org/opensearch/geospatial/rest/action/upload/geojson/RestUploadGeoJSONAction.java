/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.rest.action.upload.geojson;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.util.Arrays;
import java.util.List;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONAction;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONRequest;
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
     *   "index": "create_new_index",
     *   "field" : "geospatial field name",
     *   "type" : "geospatial field type",
     *   "data" : [
     *    {
     *       "type": "Feature",
     *       "geometry": {
     *          "type": "Polygon",
     *          "coordinates": [
     *                [
     *                    [100.0, 0.0],
     *                    [101.0, 0.0],
     *                    [101.0, 1.0],
     *                    [100.0, 1.0],
     *                    [100.0, 0.0]
     *                ]
     *           ]
     *       },
     *       "properties": {
     *          "prop0": "value0",
     *          "prop1": {
     *             "this": "that"
     *          }
     *      }
     *   }
     *  ]
     * }
     * PUT /_plugins/geospatial/geojson/_upload
     * {
     *   "index": "create_new_index_if_does_not_exists",
     *   ....... same as POST ..........
     * }
     * The difference between PUT and POST is how index existence is tolerated.
     * For POST, index should not exist, if found exists, operation will fail.
     * For PUT, index existence doesn't matter, it will create if it doesn't exists.
     */
    @Override
    public List<Route> routes() {
        String path = String.join(URL_DELIMITER, GeospatialPlugin.getPluginURLPrefix(), ACTION_OBJECT, ACTION_UPLOAD);
        return unmodifiableList(Arrays.asList(new Route(POST, path), new Route(PUT, path)));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) {
        Tuple<XContentType, BytesReference> sourceTuple = restRequest.contentOrSourceParam();
        RestRequest.Method method = restRequest.getHttpRequest().method();
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(method, sourceTuple.v2());
        return channel -> client.execute(UploadGeoJSONAction.INSTANCE, request, new RestToXContentListener<>(channel));
    }
}
