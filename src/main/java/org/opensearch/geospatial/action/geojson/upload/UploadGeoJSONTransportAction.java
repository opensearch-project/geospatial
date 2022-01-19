/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.inject.Inject;
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
        // TODO https://github.com/opensearch-project/geospatial/issues/28 (Add logic to execute import action)
        actionListener.onResponse(new AcknowledgedResponse(true));
    }
}
