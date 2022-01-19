/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

public class UploadGeoJSONAction extends ActionType<AcknowledgedResponse> {

    public static UploadGeoJSONAction INSTANCE = new UploadGeoJSONAction();
    public static final String NAME = "cluster:admin/upload_geojson_action";

    private UploadGeoJSONAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
