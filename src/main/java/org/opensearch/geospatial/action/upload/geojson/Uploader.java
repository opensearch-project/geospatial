/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.collect.MapBuilder;

/**
 * Uploader will upload GeoJSON objects from UploadGeoJSONRequestContent as
 * Documents to given index in three stage.
 * At first stage (preUpload), resources like index, mapping, pipeline with
 * GeoJSON Feature processors, will be created.
 * At second stage (upload), Feature will be extracted from GeoJSON and indexed using
 * BulkAction. This supports both Feature and FeatureCollection.
 * At third stage (postUpload), previously created pipeline will be deleted
 * At final stage response or failure will be added to the listener.
 */
public class Uploader {

    public static final String ERROR_MESSAGE_DELIMITER = "\n";

    private static final Logger LOGGER = LogManager.getLogger(Uploader.class);

    private final IndexManager indexManager;
    private final PipelineManager pipelineManager;
    private final ContentBuilder contentBuilder;

    /**
     * Uploads {@link UploadGeoJSONRequestContent#getData()}
     * @param indexManager {@link IndexManager} instance to perform index based operations
     * @param pipelineManager {@link PipelineManager} instance to perform Pipeline operations
     * @param contentBuilder {@link ContentBuilder} instance to prepare BulkRequest
     */
    public Uploader(final IndexManager indexManager, final PipelineManager pipelineManager, final ContentBuilder contentBuilder) {

        this.indexManager = Objects.requireNonNull(indexManager, "IndexManager instance cannot be null");
        this.pipelineManager = Objects.requireNonNull(pipelineManager, "PipelineManager instance cannot be null");
        this.contentBuilder = Objects.requireNonNull(contentBuilder, "ContentBuilder instance cannot be null");
    }

    /**
     * upload abstracts following operations from request
     * 1. Create index if it doesn't exist.
     * 2. Create pipeline with {@link org.opensearch.geospatial.processor.FeatureProcessor}
     * 3. Prepare Content from {@link UploadGeoJSONRequestContent#getData()}
     * 4. Upload content
     * 5. Delete pipeline
     * @param content {@link UploadGeoJSONRequestContent} derived from {@link UploadGeoJSONRequest}
     * @param isIndexAlreadyExists confirms whether the uploader should create the new index or not
     * @param flowListener action listener that contains the response of upload action.
     */
    public void upload(
        final UploadGeoJSONRequestContent content,
        final boolean isIndexAlreadyExists,
        final ActionListener<AcknowledgedResponse> flowListener
    ) {
        // validate input
        Objects.requireNonNull(flowListener, "listener cannot be null");
        Objects.requireNonNull(content, "content cannot be null");

        // initialize step listeners to chain steps
        final StepListener<Void> createIndexStep = new StepListener<>();
        final StepListener<String> createPipelineStep = new StepListener<>();
        final StepListener<Void> indexFeatureStep = new StepListener<>();
        final StepListener<Exception> deletePipelineStep = new StepListener<>();

        if (isIndexAlreadyExists) {
            LOGGER.info("Index [ " + content.getIndexName() + " ] is already exists");
            createIndexStep.onResponse(null); // mark create index step as completed, to continue to next steps.
        } else {
            // create index
            MapBuilder<String, String> fieldMap = new MapBuilder<>();
            fieldMap.put(content.getFieldName(), content.getFieldType());
            indexManager.create(content.getIndexName(), fieldMap.immutableMap(), createIndexStep);
        }
        // create a pipeline after creating index
        createIndexStep.whenComplete(
            notUsed -> pipelineManager.create(content.getFieldName(), createPipelineStep),
            flowListener::onFailure
        );

        // index features as document after creating pipeline
        createPipelineStep.whenComplete(
            pipeline -> { indexContentAsDocument(pipeline, content, indexFeatureStep); },
            createPipelineFailedException -> { flowListener.onFailure(createPipelineFailedException); }
        );

        // delete pipeline
        indexFeatureStep.whenComplete(notUsed -> {
            String pipeline = createPipelineStep.result();
            pipelineManager.delete(pipeline, deletePipelineStep, () -> null);
        }, uploadFailed -> {
            String pipeline = createPipelineStep.result();
            pipelineManager.delete(pipeline, deletePipelineStep, () -> uploadFailed);
        });

        // set response or failure depending on previous steps status
        deletePipelineStep.whenComplete(uploadFailedException -> {
            if (uploadFailedException != null) {
                throw uploadFailedException;
            }
            flowListener.onResponse(new AcknowledgedResponse(true));
        }, flowListener::onFailure);

    }

    private void indexContentAsDocument(String pipeline, UploadGeoJSONRequestContent content, StepListener<Void> uploadStepListener) {
        Optional<BulkRequestBuilder> contentRequestBuilder = contentBuilder.prepare(content, pipeline);
        if (!contentRequestBuilder.isPresent()) {
            uploadStepListener.onFailure(new IllegalStateException("No valid features are available to index"));
            return;
        }
        contentRequestBuilder.get().execute(ActionListener.wrap(bulkResponse -> {
            if (bulkResponse.hasFailures()) {
                final String failureMessage = buildUploadFailureMessage(bulkResponse);
                throw new IllegalStateException(failureMessage);
            }
            LOGGER.info("indexed " + bulkResponse.getItems().length + " features");
            uploadStepListener.onResponse(null);

        }, bulkRequestFailedException -> {
            StringBuilder message = new StringBuilder("Failed to index document due to ").append(bulkRequestFailedException.getMessage());
            uploadStepListener.onFailure(new IllegalStateException(message.toString()));
        }));
    }

    private String buildUploadFailureMessage(BulkResponse bulkResponse) {
        return Stream.of(bulkResponse.getItems())
            .filter(BulkItemResponse::isFailed)
            .map(BulkItemResponse::getFailureMessage)
            .collect(Collectors.joining(ERROR_MESSAGE_DELIMITER));
    }
}
