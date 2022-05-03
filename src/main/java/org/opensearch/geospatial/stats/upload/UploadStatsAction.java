/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.stats.upload;

import org.opensearch.action.ActionType;

public class UploadStatsAction extends ActionType<UploadStatsResponse> {

    public static final UploadStatsAction INSTANCE = new UploadStatsAction();
    public static final String NAME = "cluster:monitor/ingest/geoip/stats";

    public UploadStatsAction() {
        super(NAME, UploadStatsResponse::new);
    }
}
