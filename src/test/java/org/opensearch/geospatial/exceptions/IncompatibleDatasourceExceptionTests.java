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

public class IncompatibleDatasourceExceptionTests extends OpenSearchTestCase {
    public void testConstructor_whenCreated_thenSucceed() {
        IncompatibleDatasourceException exception = new IncompatibleDatasourceException(
            "New datasource is not compatible with existing datasource"
        );
        assertEquals(RestStatus.BAD_REQUEST, exception.status());
    }

    public void testConstructor_whenCreatedWithRootCause_thenSucceed() {
        IncompatibleDatasourceException exception = new IncompatibleDatasourceException(
            "New datasource is not compatible with existing datasource",
            new RuntimeException()
        );
        assertEquals(RestStatus.BAD_REQUEST, exception.status());
    }

    @SneakyThrows
    public void testConstructor_whenCreatedWithStream_thenSucceed() {
        IncompatibleDatasourceException exception = new IncompatibleDatasourceException(
            "New datasource is not compatible with existing datasource"
        );

        BytesStreamOutput output = new BytesStreamOutput();
        exception.writeTo(output);
        BytesStreamInput input = new BytesStreamInput(output.bytes().toBytesRef().bytes);
        IncompatibleDatasourceException copiedException = new IncompatibleDatasourceException(input);
        assertEquals(exception.getMessage(), copiedException.getMessage());
        assertEquals(exception.status(), copiedException.status());
    }
}
