/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.util.Map;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.GeospatialParser;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * TransportAction to handle import operation
 */
public class UploadGeoJSONTransportAction extends HandledTransportAction<UploadGeoJSONRequest, AcknowledgedResponse> {

    @Inject
    public UploadGeoJSONTransportAction(TransportService transportService, ActionFilters actionFilters) {
        super(UploadGeoJSONAction.NAME, transportService, actionFilters, UploadGeoJSONRequest::new);
    }

    @Override
    protected void doExecute(Task task, UploadGeoJSONRequest request, ActionListener<AcknowledgedResponse> actionListener) {
        Map<String, Object> contentAsMap = GeospatialParser.convertToMap(request.getContent());
        try {
            UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(contentAsMap);
            // TODO https://github.com/opensearch-project/geospatial/issues/28 (Add logic to execute import action)
            // 1. get index name and geospatial field from content
            // 2. create index with mapping
            // 3. Parse Data to get GeoJSON Object
            // 4. Get features from GeoJSON
            // 5. create Pipeline with GeoJSONFeatureProcessor
            // 6. Create BulkIndex Request with features as documents.
            actionListener.onResponse(new AcknowledgedResponse(true));
        } catch (Exception e) {
            actionListener.onFailure(e);
        }
    }
}
