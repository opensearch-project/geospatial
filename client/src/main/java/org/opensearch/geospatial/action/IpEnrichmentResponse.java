/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

/**
 * Wrapper class to encapsulate the IP enrichment result for IpEnrichmentTransportAction.
 */
@Getter
@Setter
@Log4j2
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class IpEnrichmentResponse extends ActionResponse {

    private Map<String, Object> geoLocationData;

    /**
     * Public method to be called by fromActionResponse( ) to populate this Response class.
     * @param streamInput Stream object which contain the geoLocationData.
     * @throws IOException Exception being thrown when given stremInput doesn't contain what IpEnrichmentResponse is expecting.
     */
    public IpEnrichmentResponse(StreamInput streamInput) throws IOException {
        super(streamInput);
        geoLocationData = streamInput.readMap();
        log.trace("Constructing IP Enrichment response with values: [{}]", geoLocationData);
    }

    /**
     * Overridden method used by OpenSearch runtime to serialise this class content into stream.
     * @param streamOutput the streamOutput used to construct this response object.
     * @throws IOException the IOException.
     */
    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeMap(geoLocationData);
    }

    /**
     * Static method to convert a given ActionResponse to IpEnrichmentResponse by serialisation with streamOuput.
     * This will be required for cross plugin communication scenario, as multiple class definition will be loaded
     * by respective Plugin's classloader.
     * @param actionResponse An IpEnrichmentResponse in casted-up form.
     * @return An IpEnrichmentResponse object which contain the same payload as the incoming object.
     */
    public static IpEnrichmentResponse fromActionResponse(ActionResponse actionResponse) {
        // From the same classloader
        if (actionResponse instanceof IpEnrichmentResponse) {
            return (IpEnrichmentResponse) actionResponse;
        }

        // Or else convert it
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (StreamInput input = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new IpEnrichmentResponse(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to parse ActionResponse into IpEnrichmentResponse", e);
        }
    }

}
