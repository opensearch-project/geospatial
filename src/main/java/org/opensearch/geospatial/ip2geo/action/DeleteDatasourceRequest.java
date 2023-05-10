/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

/**
 * GeoIP datasource delete request
 */
@Getter
@Setter
@AllArgsConstructor
public class DeleteDatasourceRequest extends ActionRequest {
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
        if (name == null || name.isBlank()) {
            errors = new ActionRequestValidationException();
            errors.addValidationError("Datasource name should not be empty");
        }
        return errors;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
    }
}
