/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.geojson.upload;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;

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

    public Map<String, Object> contentAsMap() {
        return XContentHelper.convertToMap(content, false, XContentType.JSON).v2();
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
