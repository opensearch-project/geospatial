/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.master.AcknowledgedResponse;

/**
 * Ip2Geo datasource creation action
 */
public class PutDatasourceAction extends ActionType<AcknowledgedResponse> {
    /**
     * Put datasource action instance
     */
    public static final PutDatasourceAction INSTANCE = new PutDatasourceAction();
    /**
     * Put datasource action name
     */
    public static final String NAME = "cluster:admin/geospatial/datasource/put";

    private PutDatasourceAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
