/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
