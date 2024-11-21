/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

@Getter
@Setter
@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class IpEnrichmentResponse extends ActionResponse {

    private String answer;

    public IpEnrichmentResponse(StreamInput streamInput) throws IOException {
        super(streamInput);
        answer = streamInput.readString();
    }

    @Override
    public void writeTo(StreamOutput streamOutput) throws IOException {
        streamOutput.writeString(answer);
    }

    public static IpEnrichmentResponse fromActionResponse(ActionResponse actionResponse) {
        // From the same classloader
        if (actionResponse instanceof IpEnrichmentResponse) {
            return (IpEnrichmentResponse) actionResponse;
        }

        // Or else convert it
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             OutputStreamStreamOutput osso = new OutputStreamStreamOutput(baos)) {
            actionResponse.writeTo(osso);
            try (StreamInput input =
                         new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()))) {
                return new IpEnrichmentResponse(input);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to parse ActionResponse into IpEnrichmentResponse", e);
        }
    }

    @Override
    public String toString() {
        return "IpEnrichmentResponse{" +
                "answer='" + answer + '\'' +
                '}';
    }
}
