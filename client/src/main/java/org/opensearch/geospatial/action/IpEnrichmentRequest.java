/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Wrapper for the IP 2 GeoLocation action request.
 */
@Getter
@Setter
@Log4j2
@AllArgsConstructor
public class IpEnrichmentRequest extends ActionRequest {

    private String ipString;

    private String datasourceName;

    /**
     * Constructor for TransportAction.
     * @param streamInput the streamInput.
     */
    public IpEnrichmentRequest(StreamInput streamInput) throws IOException {
        super(streamInput);
        ipString = streamInput.readString();
        datasourceName = streamInput.readString();
        log.trace("Constructing IP Enrichment request with values: [{}, {}]", ipString, datasourceName);
    }

    /**
     * Perform validation on the request, before GetSpatial processing it.
     * @return Exception which contain validation errors, if any.
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException errors = null;
        if (ipString == null) {
            errors = new ActionRequestValidationException();
            errors.addValidationError("ip string should not be null");
        }
        return errors;
    }

    /**
     * Overridden method to populate object's payload into StreamOutput form.
     * @param out the StreamOutput object.
     * @throws IOException If given StreamOutput is not compatible.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(ipString);
        out.writeOptionalString(datasourceName);
    }

    /**
     * Static method get around the cast exception happen for cross plugin communication.
     * @param actionRequest A casted-up version of IpEnrichmentRequest.
     * @return IpEnrichmentRequest object which can be used within the scope of the caller.
     */
    public static IpEnrichmentRequest fromActionRequest(ActionRequest actionRequest) {
        // From the same classloader
        if (actionRequest instanceof IpEnrichmentRequest) {
            return (IpEnrichmentRequest) actionRequest;
        }

        // Or else convert it
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionRequest.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new IpEnrichmentRequest(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse ActionRequest into IpEnrichmentRequest", e);
        }
    }
}
