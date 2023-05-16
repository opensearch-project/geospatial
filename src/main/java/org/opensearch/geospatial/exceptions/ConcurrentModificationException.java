/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.exceptions;

import java.io.IOException;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

/**
 * General ConcurrentModificationException corresponding to the {@link RestStatus#BAD_REQUEST} status code
 *
 * The exception is thrown when multiple mutation API is called for a same resource at the same time
 */
public class ConcurrentModificationException extends OpenSearchException {

    public ConcurrentModificationException(String msg, Object... args) {
        super(msg, args);
    }

    public ConcurrentModificationException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public ConcurrentModificationException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
