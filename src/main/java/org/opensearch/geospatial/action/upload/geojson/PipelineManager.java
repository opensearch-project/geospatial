/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Pipeline;

/**
 * PipelineManager is responsible for managing pipeline operations like create and delete
 */
public class PipelineManager {

    private final Logger logger = LogManager.getLogger(PipelineManager.class);

    private final ClusterAdminClient client;

    /**
     * Pipeline operations are cluster operations, hence we need {@link ClusterAdminClient} to manage
     * @param client {@link ClusterAdminClient}
     */
    public PipelineManager(ClusterAdminClient client) {
        this.client = Objects.requireNonNull(client, "Cluster admin client cannot be null");
    }

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
        DeletePipelineRequest pipelineRequest = new DeletePipelineRequest(pipeline);
        client.deletePipeline(pipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
            StringBuilder message = new StringBuilder("Deleted pipeline: ").append(pipeline);
            logger.info(message.toString());
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
        BytesReference pipelineRequestBodyBytes = BytesReference.bytes(pipelineRequestXContent);
        PutPipelineRequest pipelineRequest = new PutPipelineRequest(pipelineID, pipelineRequestBodyBytes, XContentType.JSON);
        client.putPipeline(pipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
            StringBuilder pipelineMessage = new StringBuilder("Created pipeline: ").append(pipelineID);
            logger.info(pipelineMessage.toString());
            createPipelineStep.onResponse(pipelineID);
        }, putPipelineRequestFailedException -> {
            StringBuilder message = new StringBuilder("Failed to create the pipeline: ").append(pipelineID)
                .append(" due to ")
                .append(putPipelineRequestFailedException.getMessage());
            createPipelineStep.onFailure(new IllegalStateException(message.toString()));
        }));
    }

}
