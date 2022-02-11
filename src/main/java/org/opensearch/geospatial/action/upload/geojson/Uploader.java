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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.Client;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.GeospatialParser;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.ingest.Pipeline;

/**
 * Uploader will upload GeoJSON objects from UploadGeoJSONRequestContent as
 * Documents to given index in three stage.
 * At first stage (preUpload), resources like index, mapping, pipeline with
 * GeoJSON Feature processors, will be created.
 * At second stage (upload), Feature will be extracted from GeoJSON and indexed using
 * BulkAction. This supports both Feature and FeatureCollection.
 * At third stage (postUpload), previously created pipeline will be deleted and
 * response will be added to the listener.
 */
public class Uploader {

    public static final String FIELD_TYPE_KEY = "type";
    public static final String MAPPING_PROPERTIES_KEY = "properties";
    public static final String DOCUMENT_TYPE = "_doc";
    public static final String ERROR_MESSAGE_DELIMITER = "\n";
    public static final String SPACE = " ";
    public static final String GEOJSON_FEATURE_ID_FIELD = "id";

    private final Logger logger = LogManager.getLogger(Uploader.class);

    private final Client client;
    private final ActionListener<AcknowledgedResponse> flowListener;

    public Uploader(Client client, ActionListener<AcknowledgedResponse> flowListener) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
        this.flowListener = Objects.requireNonNull(flowListener, "listener cannot be null");
    }

    /**
     * upload abstracts following operations from request
     * 1. Create index if it doesn't exist ( preUpload)
     * 2. Create pipeline ( preUpload)
     * 3. Create BulkRequest, with every {@link UploadGeoJSONRequestContent#getData()} as {@link IndexRequestBuilder} (upload)
     * 4. Execute {@link org.opensearch.action.bulk.BulkAction} (upload)
     * 5. Delete pipeline (postUpload)
     * @param content {@link UploadGeoJSONRequestContent} derived from {@link UploadGeoJSONRequest}
     * @param isIndexAlreadyExists confirms whether the uploader should create the new index or not
     * @throws IOException if upload fails to build mapping or pipeline body.
     */
    public void upload(UploadGeoJSONRequestContent content, boolean isIndexAlreadyExists) throws IOException {
        StepListener<String> preUploadStepListener = new StepListener<>();
        StepListener<Void> uploadListener = new StepListener<>();

        preUploadStep(content, isIndexAlreadyExists, preUploadStepListener);

        // index features as document after creating pipeline, delete pipeline if it fails
        preUploadStepListener.whenComplete(
            pipeline -> { upload(pipeline, content, uploadListener); },
            uploadFailedException -> { postUploadStep(preUploadStepListener.result(), uploadFailedException); }
        );

        uploadListener.whenComplete(unUsed -> { postUploadStep(preUploadStepListener.result(), null); }, flowListener::onFailure);

    }

    private void preUploadStep(
        UploadGeoJSONRequestContent content,
        boolean isIndexAlreadyExists,
        StepListener<String> preUploadActionListener
    ) throws IOException {
        StepListener<Void> createIndexActionListener = new StepListener<>();
        if (!isIndexAlreadyExists) {
            // create index
            XContentBuilder newIndexMapping = buildMapping(content.getFieldName(), content.getFieldType());
            createIndex(content.getIndexName(), newIndexMapping, createIndexActionListener);
        } else {
            createIndexActionListener.onResponse(null); // mark step 1 as completed, to continue to next steps.
        }
        // create pipeline after creating index
        XContentBuilder processorConfig = buildProcessors(content.getFieldName());
        createIndexActionListener.whenComplete(unUsed -> createPipeline(processorConfig, preUploadActionListener), flowListener::onFailure);
    }

    private void postUploadStep(String pipeline, Exception indexGeoJSONFailedException) {
        deletePipeline(pipeline, indexGeoJSONFailedException);
    }

    private void deletePipeline(String pipeline, Exception indexGeoJSONFailedException) {
        DeletePipelineRequest pipelineRequest = new DeletePipelineRequest(pipeline);
        client.admin().cluster().deletePipeline(pipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
            StringBuilder message = new StringBuilder("Deleted pipeline: ").append(pipeline);
            logger.info(message.toString());
            if (indexGeoJSONFailedException != null) {
                flowListener.onFailure(indexGeoJSONFailedException);
                return;
            }
            flowListener.onResponse(new AcknowledgedResponse(true));
        }, deletePipelineFailedException -> {
            StringBuilder message = new StringBuilder("Failed to delete the pipeline: ").append(pipeline)
                .append(SPACE)
                .append("due to")
                .append(SPACE)
                .append(deletePipelineFailedException.getMessage());
            flowListener.onFailure(new IllegalStateException(message.toString()));
        }));
    }

    /* builds following content
         {
             "properties": {
                "field_name": {
                   "type": "geospatial_field_type"
                 }
             }
          }
    */
    private XContentBuilder buildMapping(String geoSpatialFieldName, String geoSpatialFieldType) throws IOException {

        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject(MAPPING_PROPERTIES_KEY)
            .startObject(geoSpatialFieldName)
            .field(FIELD_TYPE_KEY, geoSpatialFieldType)
            .endObject()
            .endObject()
            .endObject();
    }

    private void createIndex(String indexName, XContentBuilder mapping, StepListener<Void> createIndexStep) {
        CreateIndexRequest request = new CreateIndexRequest(indexName).mapping(DOCUMENT_TYPE, mapping);
        client.admin().indices().create(request, ActionListener.wrap(createIndexResponse -> {
            StringBuilder message = new StringBuilder("Created index: ").append(indexName);
            logger.info(message.toString());
            // notify the listener to call next steps
            createIndexStep.onResponse(null);
        }, createIndexFailedException -> {
            StringBuilder message = new StringBuilder("Failed to create the index: ").append(indexName)
                .append(SPACE)
                .append("due to")
                .append(SPACE)
                .append(createIndexFailedException.getMessage());
            // notify the listener that failure happened and discard rest of the steps.
            createIndexStep.onFailure(new IllegalStateException(message.toString()));
        }));
    }

    /* builds following content
     {
         "description" : "Ingest GeoJSON into index",
         "processors" : [
           {
               "geojson-feature" : {
                  "field" : "geospatial_field_name"
                }
            }
         ]
      }
     */
    private XContentBuilder buildProcessors(String fieldName) throws IOException {

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

    private void createPipeline(XContentBuilder pipelineRequestXContent, StepListener<String> createPipelineStep) {
        BytesReference pipelineRequestBodyBytes = BytesReference.bytes(pipelineRequestXContent);
        final String pipelineID = UUIDs.randomBase64UUID();
        PutPipelineRequest pipelineRequest = new PutPipelineRequest(pipelineID, pipelineRequestBodyBytes, XContentType.JSON);
        client.admin().cluster().putPipeline(pipelineRequest, ActionListener.wrap(acknowledgedResponse -> {
            StringBuilder pipelineMessage = new StringBuilder("Created pipeline: ").append(pipelineID);
            logger.info(pipelineMessage.toString());
            createPipelineStep.onResponse(pipelineID);
        }, createPipelineFailedException -> {
            StringBuilder message = new StringBuilder("Failed to create the pipeline: ").append(pipelineID)
                .append(SPACE)
                .append("due to")
                .append(SPACE)
                .append(createPipelineFailedException.getMessage());
            // notify the listener that failure happened and discard rest of the steps.
            createPipelineStep.onFailure(new IllegalStateException(message.toString()));
        }));
    }

    private IndexRequestBuilder createIndexRequestBuilder(Map<String, Object> source) {
        final IndexRequestBuilder requestBuilder = client.prepareIndex().setSource(source);
        String id = GeospatialParser.extractValueAsString(source, GEOJSON_FEATURE_ID_FIELD);
        return Strings.hasText(id) ? requestBuilder.setId(id) : requestBuilder;
    }

    private BulkRequestBuilder getBulkRequestBuilder() {
        return client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
    }

    /*
        build BulkRequestBuilder, by, iterating, UploadGeoJSONRequestContent's data. This depends on
        GeospatialParser.getFeatures to extract features from user input, create IndexRequestBuilder
        with index name and pipeline.
     */
    private BulkRequestBuilder buildBulkRequestBuilder(UploadGeoJSONRequestContent content, String pipeline) {
        BulkRequestBuilder builder = getBulkRequestBuilder();
        List<Object> documents = (List<Object>) content.getData();
        documents.stream()
            .map(GeospatialParser::toStringObjectMap)
            .map(GeospatialParser::getFeatures)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .flatMap(List::stream)
            .map(documentSource -> createIndexRequestBuilder(documentSource))
            .map(indexRequestBuilder -> indexRequestBuilder.setIndex(content.getIndexName()))
            .map(indexRequestBuilder -> indexRequestBuilder.setPipeline(pipeline))
            .map(indexRequestBuilder -> indexRequestBuilder.setType(DOCUMENT_TYPE))
            .forEach(builder::add);
        return builder;
    }

    /*
        upload user input into index using pipeline for processing
    */
    private void upload(String pipeline, UploadGeoJSONRequestContent content, StepListener<Void> uploadStepListener) {
        BulkRequestBuilder bulkRequestBuilder = buildBulkRequestBuilder(content, pipeline);

        if (bulkRequestBuilder.numberOfActions() < 1) { // check any features to upload
            // mark step as failed
            uploadStepListener.onFailure(new IllegalArgumentException("No valid features are available to upload"));
            return;
        }

        bulkRequestBuilder.execute(ActionListener.wrap(bulkResponse -> {
            if (bulkResponse.hasFailures()) {
                final String failureMessage = buildBulkRequestFailureMessage(bulkResponse);
                throw new IllegalStateException(failureMessage);
            }
            logger.info("indexed " + bulkResponse.getItems().length + " features");
            uploadStepListener.onResponse(null);

        }, bulkRequestFailedException -> {
            StringBuilder message = new StringBuilder("Failed to index document due to ").append(bulkRequestFailedException.getMessage());
            // notify the listener that failure happened and discard rest of the steps.
            uploadStepListener.onFailure(new IllegalStateException(message.toString()));
        }));
    }

    /*
      builds failure message from BulkItemResponse
     */
    private String buildBulkRequestFailureMessage(BulkResponse bulkResponse) {
        List<BulkItemResponse> failedResponse = new ArrayList<>();
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (response.isFailed()) {
                failedResponse.add(response);
            }
        }
        return failedResponse.stream().map(BulkItemResponse::getFailureMessage).collect(Collectors.joining(ERROR_MESSAGE_DELIMITER));
    }

}
