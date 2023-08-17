/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.StepListener;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.core.action.ActionListener;
import org.opensearch.geospatial.stats.upload.UploadMetric;
import org.opensearch.geospatial.stats.upload.UploadStats;

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

    private static final Logger LOGGER = LogManager.getLogger(Uploader.class);
    private static final String GEOJSON = "geojson";

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
        final ActionListener<UploadGeoJSONResponse> flowListener
    ) {
        // validate input
        Objects.requireNonNull(flowListener, "listener cannot be null");
        Objects.requireNonNull(content, "content cannot be null");

        // initialize step listeners to chain steps
        final StepListener<Void> createIndexStep = new StepListener<>();
        final StepListener<String> createPipelineStep = new StepListener<>();
        final StepListener<BulkResponse> indexFeatureStep = new StepListener<>();
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
        createPipelineStep.whenComplete(pipeline -> indexContentAsDocument(pipeline, content, indexFeatureStep), flowListener::onFailure);

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
            BulkResponse response = indexFeatureStep.result();
            createAndAddMetricToStats(createPipelineStep.result(), response);
            flowListener.onResponse(new UploadGeoJSONResponse(response));
        }, deletePipelineFailed -> {
            try {
                BulkResponse response = indexFeatureStep.result();
                createAndAddMetricToStats(createPipelineStep.result(), response);
                // TODO Propagate deletePipelineFailed exception to response as low severity error
                flowListener.onResponse(new UploadGeoJSONResponse(response));
            } catch (IllegalStateException stepFailed) {
                flowListener.onFailure(deletePipelineFailed);
            }
        });

    }

    private void indexContentAsDocument(
        String pipeline,
        UploadGeoJSONRequestContent content,
        StepListener<BulkResponse> uploadStepListener
    ) {
        Optional<BulkRequestBuilder> contentRequestBuilder = contentBuilder.prepare(content, pipeline);
        if (contentRequestBuilder.isEmpty()) {
            uploadStepListener.onFailure(new IllegalStateException("No valid features are available to index"));
            return;
        }
        contentRequestBuilder.get()
            .execute(
                ActionListener.wrap(
                    uploadStepListener::onResponse,
                    bulkRequestFailedException -> uploadStepListener.onFailure(
                        new IllegalStateException("Failed to index document due to " + bulkRequestFailedException.getMessage())
                    )
                )
            );
    }

    private void createAndAddMetricToStats(String metricID, BulkResponse response) {
        UploadMetric metric = createUploadMetric(metricID, response);
        UploadStats.getInstance().addMetric(metric);
    }

    private UploadMetric createUploadMetric(String id, BulkResponse response) {
        UploadMetric.UploadMetricBuilder metricBuilder = new UploadMetric.UploadMetricBuilder(id, GEOJSON);
        BulkItemResponse[] items = response.getItems();
        metricBuilder.uploadCount(items.length);
        metricBuilder.duration(response.getTook().duration());
        if (response.hasFailures()) {
            return createFailedUploadMetric(metricBuilder, items);
        }
        return createSuccessUploadMetric(metricBuilder, items);
    }

    private UploadMetric createSuccessUploadMetric(UploadMetric.UploadMetricBuilder metricBuilder, BulkItemResponse[] items) {
        Objects.requireNonNull(metricBuilder, "metric builder cannot be null");
        Objects.requireNonNull(items, "BulkItemResponse array cannot be null");
        metricBuilder.successCount(items.length);
        return metricBuilder.build();
    }

    private UploadMetric createFailedUploadMetric(UploadMetric.UploadMetricBuilder metricBuilder, BulkItemResponse[] items) {
        Objects.requireNonNull(metricBuilder, "metric builder cannot be null");
        Objects.requireNonNull(items, "BulkItemResponse cannot be null");
        long failed = Arrays.stream(items).filter(BulkItemResponse::isFailed).count();
        metricBuilder.failedCount(failed);
        long success = items.length - failed;
        metricBuilder.successCount(success);
        return metricBuilder.build();
    }
}
