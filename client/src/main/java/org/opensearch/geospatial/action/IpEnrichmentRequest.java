/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public class IpEnrichmentRequest extends ActionRequest {

    private String ipString;


    public IpEnrichmentRequest() {
    }

    public IpEnrichmentRequest(String ipString) {
        this.ipString = ipString;
    }

    /**
     * Constructor for TransportAction.
     * @param streamInput
     */
    public IpEnrichmentRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
        ipString = streamInput.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = null;
        if (ipString == null) {
            errors = new ActionRequestValidationException();
            errors.addValidationError("ip string should not be null");
        }
        return errors;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(ipString);
    }

    public String getIpString() {
        return ipString;
    }

    public static IpEnrichmentRequest fromActionRequest(ActionRequest actionRequest) {
        // From the same classloader
        if (actionRequest instanceof IpEnrichmentRequest) {
            return (IpEnrichmentRequest) actionRequest;
        }

        // Or else convert it
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input =
                         new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new IpEnrichmentRequest(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionRequest into IpEnrichmentRequest", e);
        }
    }
}
