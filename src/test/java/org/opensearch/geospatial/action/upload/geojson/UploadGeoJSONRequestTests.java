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

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.json.JSONObject;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestTests extends OpenSearchTestCase {

    private String getRequestBody(Map<String, Object> contentMap) {
        JSONObject json = new JSONObject();
        if (contentMap == null) {
            return json.toString();
        }
        for (Map.Entry<String, Object> entry : contentMap.entrySet()) {
            json.put(entry.getKey(), entry.getValue());
        }
        return json.toString();
    }

    public void testStreams() throws IOException {
        String requestBody = getRequestBody(null);
        RestRequest.Method method = PUT;
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(method, new BytesArray(requestBody.getBytes(StandardCharsets.UTF_8)));
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);
        StreamInput in = StreamInput.wrap(output.bytes().toBytesRef().bytes);

        UploadGeoJSONRequest serialized = new UploadGeoJSONRequest(in);
        assertEquals(requestBody, serialized.getContent().utf8ToString());
        assertEquals(method, serialized.getMethod());
    }

    public void testRequestValidation() {
        UploadGeoJSONRequest request = new UploadGeoJSONRequest(
            POST,
            new BytesArray(getRequestBody(null).getBytes(StandardCharsets.UTF_8))
        );
        assertNull(request.validate());
    }
}
