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

public class ResourceInUseExceptionTests extends OpenSearchTestCase {
    public void testConstructor_whenCreated_thenSucceed() {
        ResourceInUseException exception = new ResourceInUseException("Resource is in use");
        assertEquals(RestStatus.BAD_REQUEST, exception.status());
    }

    public void testConstructor_whenCreatedWithRootCause_thenSucceed() {
        ResourceInUseException exception = new ResourceInUseException("Resource is in use", new RuntimeException());
        assertEquals(RestStatus.BAD_REQUEST, exception.status());
    }

    @SneakyThrows
    public void testConstructor_whenCreatedWithStream_thenSucceed() {
        ResourceInUseException exception = new ResourceInUseException("New datasource is not compatible with existing datasource");

        BytesStreamOutput output = new BytesStreamOutput();
        exception.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        ResourceInUseException copiedException = new ResourceInUseException(input);
        assertEquals(exception.getMessage(), copiedException.getMessage());
        assertEquals(exception.status(), copiedException.status());
    }
}
