/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.json.JSONObject;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

public class UploadGeoJSONRequestTests extends OpenSearchTestCase {

    private String getRandomRequestBody() {
        JSONObject json = new JSONObject();
        json.put(randomLowerCaseString(), randomLowerCaseString());
        return json.toString();
    }

    public void testStreams() throws IOException {
        String requestBody = getRandomRequestBody();
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
            new BytesArray(getRandomRequestBody().getBytes(StandardCharsets.UTF_8))
        );
        assertNull(request.validate());
    }
}
