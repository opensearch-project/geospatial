/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;

import lombok.Getter;
import lombok.Setter;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

/**
 * Ip2Geo datasource get request
 */
@Getter
@Setter
public class GetDatasourceRequest extends ActionRequest {
    /**
     * @param names the datasource names
     * @return the datasource names
     */
    private String[] names;

    /**
     * Constructs a new get datasource request with a list of datasources.
     *
     * If the list of datasources is empty or it contains a single element "_all", all registered datasources
     * are returned.
     *
     * @param names list of datasource names
     */
    public GetDatasourceRequest(final String[] names) {
        this.names = names;
    }

    /**
     * Constructor with stream input
     * @param in the stream input
     * @throws IOException IOException
     */
    public GetDatasourceRequest(final StreamInput in) throws IOException {
        super(in);
        this.names = in.readStringArray();
    }

    @Override
    public ActionRequestValidationException validate() {
        if (names == null) {
            throw new OpenSearchException("names should not be null");
        }
        return null;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(names);
    }
}
