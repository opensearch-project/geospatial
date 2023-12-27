/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.geospatial.ip2geo.common.ParameterValidator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * GeoIP datasource delete request
 */
@Getter
@Setter
@AllArgsConstructor
public class DeleteDatasourceRequest extends ActionRequest {
    private static final ParameterValidator VALIDATOR = new ParameterValidator();
    /**
     * @param name the datasource name
     * @return the datasource name
     */
    private String name;

    /**
     * Constructor
     *
     * @param in the stream input
     * @throws IOException IOException
     */
    public DeleteDatasourceRequest(final StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = null;
        if (VALIDATOR.validateDatasourceName(name).isEmpty() == false) {
            errors = new ActionRequestValidationException();
            errors.addValidationError("no such datasource exist");
        }
        return errors;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }
}
