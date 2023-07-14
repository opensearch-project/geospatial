/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.exceptions;

import lombok.SneakyThrows;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

public class ConcurrentModificationExceptionTests extends OpenSearchTestCase {
    public void testConstructor_whenCreated_thenSucceed() {
        ConcurrentModificationException exception = new ConcurrentModificationException("Resource is being modified by another processor");
        assertEquals(RestStatus.BAD_REQUEST, exception.status());
    }

    public void testConstructor_whenCreatedWithRootCause_thenSucceed() {
        ConcurrentModificationException exception = new ConcurrentModificationException(
            "Resource is being modified by another processor",
            new RuntimeException()
        );
        assertEquals(RestStatus.BAD_REQUEST, exception.status());
    }

    @SneakyThrows
    public void testConstructor_whenCreatedWithStream_thenSucceed() {
        ConcurrentModificationException exception = new ConcurrentModificationException(
            "New datasource is not compatible with existing datasource"
        );

        BytesStreamOutput output = new BytesStreamOutput();
        exception.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        ConcurrentModificationException copiedException = new ConcurrentModificationException(input);
        assertEquals(exception.getMessage(), copiedException.getMessage());
        assertEquals(exception.status(), copiedException.status());
    }
}
