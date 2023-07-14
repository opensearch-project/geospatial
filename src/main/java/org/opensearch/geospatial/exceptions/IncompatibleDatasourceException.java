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
 * IncompatibleDatasourceException corresponding to the {@link RestStatus#BAD_REQUEST} status code
 *
 * The exception is thrown when a user tries to update datasource with new endpoint which is not compatible
 * with current datasource
 */
public class IncompatibleDatasourceException extends OpenSearchException {

    public IncompatibleDatasourceException(String msg, Object... args) {
        super(msg, args);
    }

    public IncompatibleDatasourceException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public IncompatibleDatasourceException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public final RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
