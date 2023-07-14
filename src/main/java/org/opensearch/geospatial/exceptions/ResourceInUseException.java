/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.exceptions;

import java.io.IOException;

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;

/**
 * Generic ResourceInUseException corresponding to the {@link RestStatus#BAD_REQUEST} status code
 */
public class ResourceInUseException extends OpenSearchException {

    public ResourceInUseException(String msg, Object... args) {
        super(msg, args);
    }

    public ResourceInUseException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public ResourceInUseException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
