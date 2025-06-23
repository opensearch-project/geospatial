/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.io.IOException;
import java.util.function.Supplier;

import org.opensearch.action.StepListener;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.common.UUIDs;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Pipeline;
import org.opensearch.transport.client.ClusterAdminClient;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

/**
 * PipelineManager is responsible for managing pipeline operations like create and delete
 */
@AllArgsConstructor
@Log4j2
public class PipelineManager {

    @NonNull
    private final ClusterAdminClient client;

    /**
     * Creates a new pipeline with GeoJSON Feature Processor
     * @param fieldName Geospatial field that is required for {@link FeatureProcessor}
     * @param createPipelineStep notify listener that publishes status of the action.
     */
    public void create(String fieldName, StepListener<String> createPipelineStep) {
        final String pipeline = UUIDs.randomBase64UUID();
        create(pipeline, fieldName, createPipelineStep);
    }

    private void create(String pipelineName, String fieldName, StepListener<String> createPipelineStep) {
        try (final XContentBuilder pipelineRequestContent = buildPipelineRequestContent(fieldName)) {
            createPipeline(pipelineRequestContent, pipelineName, createPipelineStep);
        } catch (IOException mappingException) {
            createPipelineStep.onFailure(mappingException);
        }
    }

    /**
     * Delete pipleine based on given name, notify the status of action using {@link StepListener}
     * @param pipeline pipeline name to be deleted.
     * @param deletePipelineStep  notifies the status of this action
     * @param supplier Exception to be passed to the listener.
     */
    public void delete(String pipeline, StepListener<Exception> deletePipelineStep, final Supplier<Exception> supplier) {
        final DeletePipelineRequest pipelineRequest = new DeletePipelineRequest(pipeline);
        client.deletePipeline(pipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
            StringBuilder message = new StringBuilder("Deleted pipeline: ").append(pipeline);
            log.info(message.toString());
            deletePipelineStep.onResponse(supplier.get());
        }, deletePipelineRequestFailed -> {
            StringBuilder message = new StringBuilder("Failed to delete the pipeline: ").append(pipeline)
                .append(" due to ")
                .append(deletePipelineRequestFailed.getMessage());
            if (supplier.get() != null) {
                message.append("\n").append("Another exception occurred: ").append(supplier.get().getMessage());
            }
            deletePipelineStep.onFailure(new IllegalStateException(message.toString()));
        }));
    }

    private XContentBuilder buildPipelineRequestContent(String fieldName) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray(Pipeline.PROCESSORS_KEY)
            .startObject()
            .startObject(FeatureProcessor.TYPE)
            .field(FeatureProcessor.FIELD_KEY, fieldName)
            .endObject()
            .endObject()
            .endArray()
            .endObject();
    }

    private void createPipeline(XContentBuilder pipelineRequestXContent, String pipelineID, StepListener<String> createPipelineStep) {
        final BytesReference pipelineRequestBodyBytes = BytesReference.bytes(pipelineRequestXContent);
        final PutPipelineRequest pipelineRequest = new PutPipelineRequest(pipelineID, pipelineRequestBodyBytes, XContentType.JSON);
        client.putPipeline(pipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
            StringBuilder pipelineMessage = new StringBuilder("Created pipeline: ").append(pipelineID);
            log.info(pipelineMessage.toString());
            createPipelineStep.onResponse(pipelineID);
        }, putPipelineRequestFailedException -> {
            StringBuilder message = new StringBuilder("Failed to create the pipeline: ").append(pipelineID)
                .append(" due to ")
                .append(putPipelineRequestFailedException.getMessage());
            createPipelineStep.onFailure(new IllegalStateException(message.toString()));
        }));
    }

}
