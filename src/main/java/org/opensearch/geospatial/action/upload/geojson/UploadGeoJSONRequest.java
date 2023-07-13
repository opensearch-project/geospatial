/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.io.IOException;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.rest.RestRequest;

@AllArgsConstructor
@Getter
public class UploadGeoJSONRequest extends ActionRequest {

    /**
     * Request method type. This will be useful to decide whether new index should
     * be created for upload request or not.
     */
    @NonNull
    private final RestRequest.Method method;
    @NonNull
    private final BytesReference content;

    public UploadGeoJSONRequest(StreamInput in) throws IOException {
        super(in);
        this.content = Objects.requireNonNull(in.readBytesReference(), "data is missing");
        this.method = Objects.requireNonNull(in.readEnum(RestRequest.Method.class), "RestRequest Method is missing");
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(content);
        out.writeEnum(method);
    }
}
