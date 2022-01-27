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

public class UploadGeoJSONRequest extends ActionRequest {

    private BytesReference content;

    public UploadGeoJSONRequest(BytesReference content) {
        this.content = Objects.requireNonNull(content);
    }

    public UploadGeoJSONRequest(StreamInput in) throws IOException {
        super(in);
        this.content = Objects.requireNonNull(in.readBytesReference());
    }

    public BytesReference getContent() {
        return content;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(content);
    }
}
