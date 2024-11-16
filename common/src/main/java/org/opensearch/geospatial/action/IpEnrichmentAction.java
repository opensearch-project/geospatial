/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionResponse;

public class IpEnrichmentAction extends ActionType<ActionResponse> {


    public static final IpEnrichmentAction INSTANCE = new IpEnrichmentAction();

    public static final String NAME = "cluster:admin/geospatial/ipenrichment/get";

    public IpEnrichmentAction() {
        super(NAME, IpEnrichmentResponse::new);
    }
}
