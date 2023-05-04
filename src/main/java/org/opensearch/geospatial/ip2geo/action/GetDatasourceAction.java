/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import org.opensearch.action.ActionType;

/**
 * Ip2Geo datasource get action
 */
public class GetDatasourceAction extends ActionType<GetDatasourceResponse> {
    /**
     * Get datasource action instance
     */
    public static final GetDatasourceAction INSTANCE = new GetDatasourceAction();
    /**
     * Name of a get datasource action
     */
    public static final String NAME = "cluster:admin/geospatial/datasource/get";

    private GetDatasourceAction() {
        super(NAME, GetDatasourceResponse::new);
    }
}
