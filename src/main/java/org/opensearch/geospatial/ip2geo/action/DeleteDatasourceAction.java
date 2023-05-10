/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

/**
 * Ip2Geo datasource delete action
 */
public class DeleteDatasourceAction extends ActionType<AcknowledgedResponse> {
    /**
     * Delete datasource action instance
     */
    public static final DeleteDatasourceAction INSTANCE = new DeleteDatasourceAction();
    /**
     * Delete datasource action name
     */
    public static final String NAME = "cluster:admin/geospatial/datasource/delete";

    private DeleteDatasourceAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
