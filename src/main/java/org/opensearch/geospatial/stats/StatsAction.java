/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats;

import org.opensearch.action.ActionType;

public class StatsAction extends ActionType<StatsResponse> {

    public static final StatsAction INSTANCE = new StatsAction();
    public static final String NAME = "cluster:admin/geospatial/stats";

    public StatsAction() {
        super(NAME, StatsResponse::new);
    }
}
