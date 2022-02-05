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

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.rest.RestRequest;

public class UploadGeoJSONRequest extends ActionRequest {

    private final RestRequest.Method method;
    private BytesReference content;

    public UploadGeoJSONRequest(RestRequest.Method method, BytesReference content) {
        super();
        this.method = Objects.requireNonNull(method, "method cannot be null");
        this.content = Objects.requireNonNull(content, "content cannot be null");
    }

    public UploadGeoJSONRequest(StreamInput in) throws IOException {
        super(in);
        this.content = Objects.requireNonNull(in.readBytesReference());
        this.method = in.readEnum(RestRequest.Method.class);
    }

    public BytesReference getContent() {
        return content;
    }

    /**
     * getter for Request method type. This will be useful to decide whether new index should
     * be created for upload request or not.
     * @return RestRequest.Method either PUT or POST
     */
    public RestRequest.Method getMethod() {
        return method;
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
