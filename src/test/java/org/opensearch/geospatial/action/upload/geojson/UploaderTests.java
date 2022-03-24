/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.StepListener;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.RandomObjects;

public class UploaderTests extends OpenSearchTestCase {

    public static final int MAX_NUM_ACTION = 5;
    public static final boolean ACTION_SUCCESS = true;
    public static final boolean BULK_REQUEST_SUCCESS = false;
    public static final boolean BULK_REQUEST_FAILURE = true;
    public static final boolean ACTION_FAILED = false;
    public static final int MAX_SHARD_ID = 100;
    public static final int MAX_SEQ_NO = 10000;
    public static final int MAX_PRIMARY_TERM = 10000;
    public static final int MAX_VERSION = 10000;
    public static final boolean INDEX_ALREADY_EXIST = true;
    public static final boolean INDEX_DOES_NOT_EXIST = false;
    private Uploader uploader;
    private UploadGeoJSONRequestContent content;
    private ActionListener mockListener;
    private IndexManager mockIndexManager;
    private PipelineManager mockPipelineManager;
    private ContentBuilder mockContentBuilder;
    private BulkRequestBuilder mockBulkRequestBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockListener = mock(ActionListener.class);
        mockIndexManager = mock(IndexManager.class);
        mockPipelineManager = mock(PipelineManager.class);
        mockContentBuilder = mock(ContentBuilder.class);
        mockBulkRequestBuilder = mock(BulkRequestBuilder.class);

        uploader = new Uploader(mockIndexManager, mockPipelineManager, mockContentBuilder);
        Map<String, Object> contentMap = GeospatialTestHelper.buildRequestContent(3);
        content = UploadGeoJSONRequestContent.create(contentMap);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private void mockCreateIndexAction(boolean status) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            StepListener<Void> createIndexListener = (StepListener<Void>) args[2];
            if (status) {             // call onResponse flow
                createIndexListener.onResponse(null);
                return null;
            }
            createIndexListener.onFailure(new IllegalStateException(randomLowerCaseString()));
            return null;
        }).when(mockIndexManager).create(anyString(), anyMap(), any(StepListener.class));
    }

    private void mockCreatePipelineAction(boolean status) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            StepListener<String> createPipelineListener = (StepListener<String>) args[1];
            if (status) {             // call onResponse flow
                createPipelineListener.onResponse(randomLowerCaseString());
                return null;
            }
            createPipelineListener.onFailure(new IllegalStateException(randomLowerCaseString()));
            return null;
        }).when(mockPipelineManager).create(anyString(), any(StepListener.class));
    }

    private void mockDeletePipelineAction(boolean status, Supplier<Exception> supplier) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 3;
            StepListener<Exception> createPipelineListener = (StepListener<Exception>) args[1];

            if (status) {             // call onResponse flow
                createPipelineListener.onResponse(supplier.get());
                return null;
            }
            createPipelineListener.onFailure(new IllegalStateException(randomLowerCaseString()));
            return null;
        }).when(mockPipelineManager).delete(anyString(), any(StepListener.class), any(Supplier.class));
    }

    private void mockContentPreparation(boolean status) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            if (status) {             // call onResponse flow
                return Optional.of(mockBulkRequestBuilder);
            }
            return Optional.empty();
        }).when(mockContentBuilder).prepare(any(UploadGeoJSONRequestContent.class), anyString());
    }

    public void testCreateIndexIsNotCalled() {
        uploader.upload(content, INDEX_ALREADY_EXIST, mockListener);
        verify(mockIndexManager, never()).create(anyString(), any(Map.class), any(StepListener.class));
    }

    public void testCreateIndexSuccess() {
        mockCreateIndexAction(ACTION_SUCCESS);
        uploader.upload(content, INDEX_DOES_NOT_EXIST, mockListener);
        verify(mockIndexManager).create(any(String.class), anyMap(), any(StepListener.class));
        // if create index is success, verify next step is called.
        verify(mockPipelineManager).create(anyString(), any(StepListener.class));
    }

    public void testCreateIndexFailed() {
        mockCreateIndexAction(ACTION_FAILED);
        uploader.upload(content, INDEX_DOES_NOT_EXIST, mockListener);
        verify(mockIndexManager).create(any(String.class), anyMap(), any(StepListener.class));
        // if create index is success, verify, next step is not called.
        verify(mockPipelineManager, never()).create(anyString(), any(StepListener.class));
    }

    public void testCreatePipelineSuccess() {
        mockCreatePipelineAction(ACTION_SUCCESS);
        uploader.upload(content, INDEX_ALREADY_EXIST, mockListener);
        verify(mockPipelineManager).create(anyString(), any(StepListener.class));
        // if create pipeline is success, verify next step is called.
        verify(mockContentBuilder).prepare(any(UploadGeoJSONRequestContent.class), anyString());
    }

    public void testCreatePipelineFailed() {

        mockCreatePipelineAction(ACTION_FAILED);
        uploader.upload(content, INDEX_ALREADY_EXIST, mockListener);
        // if create index is success, verify, next step is not called.
        verify(mockContentBuilder, never()).prepare(any(UploadGeoJSONRequestContent.class), anyString());
    }

    public void testBulkActionWithoutFailures() {
        mockCreatePipelineAction(ACTION_SUCCESS);
        mockContentPreparation(ACTION_SUCCESS);
        mockBulkRequestExecute(MAX_NUM_ACTION, BULK_REQUEST_SUCCESS);
        mockDeletePipelineAction(ACTION_SUCCESS, () -> null);
        uploader.upload(content, INDEX_ALREADY_EXIST, mockListener);
        verify(mockBulkRequestBuilder).execute(any(ActionListener.class));
        verify(mockPipelineManager).delete(anyString(), any(StepListener.class), any(Supplier.class));
        verify(mockListener).onResponse(any());
    }

    //
    public void testBulkActionWithFailedIndexRequest() {

        mockCreatePipelineAction(ACTION_SUCCESS);
        mockContentPreparation(ACTION_SUCCESS);
        mockBulkRequestExecute(MAX_NUM_ACTION, BULK_REQUEST_FAILURE);
        mockDeletePipelineAction(ACTION_SUCCESS, () -> new IllegalStateException(randomLowerCaseString()));
        uploader.upload(content, INDEX_ALREADY_EXIST, mockListener);
        verify(mockBulkRequestBuilder).execute(any(ActionListener.class));
        verify(mockPipelineManager).delete(anyString(), any(StepListener.class), any(Supplier.class));
        verify(mockListener).onFailure(any());
    }

    private void mockBulkRequestExecute(int noOfActions, boolean hasFailures) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 1;
            ActionListener<BulkResponse> bulkRequestAction = (ActionListener<BulkResponse>) args[0];
            bulkRequestAction.onResponse(generateRandomBulkResponse(noOfActions, hasFailures));
            return null;
        }).when(mockBulkRequestBuilder).execute(any(ActionListener.class));
    }

    private BulkResponse generateRandomBulkResponse(int noOfItems, boolean hasFailures) {
        long took = randomNonNegativeLong();
        long ingestTook = randomNonNegativeLong();
        if (noOfItems < 1) {
            return new BulkResponse(null, took, ingestTook);
        }
        List<BulkItemResponse> items = new ArrayList<>();
        IntStream.range(0, noOfItems)
            .forEach(shardId -> items.add(new BulkItemResponse(shardId, DocWriteRequest.OpType.CREATE, randomIndexResponse())));
        if (hasFailures) {
            final BulkItemResponse.Failure failedToIndex = new BulkItemResponse.Failure(
                randomLowerCaseString(),
                randomLowerCaseString(),
                new OpenSearchException(randomLowerCaseString())
            );
            items.add(new BulkItemResponse(randomIntBetween(0, MAX_SHARD_ID), DocWriteRequest.OpType.CREATE, failedToIndex));
        }
        return new BulkResponse(items.stream().toArray(BulkItemResponse[]::new), took, ingestTook);
    }

    /**
     * Returns random @link IndexResponse}s by generating inputs using random functions.
     * It is not guaranted to generate every possible values, and it is not required since
     * it is used by the unit test and will not be validated by the cluster.
     */
    public static IndexResponse randomIndexResponse() {
        String index = randomLowerCaseString();
        String indexUUid = UUIDs.randomBase64UUID();
        int shardId = randomIntBetween(0, MAX_SHARD_ID);
        String id = UUIDs.randomBase64UUID();
        long seqNo = randomIntBetween(0, MAX_SEQ_NO);
        long primaryTerm = randomIntBetween(0, MAX_PRIMARY_TERM);
        long version = randomIntBetween(0, MAX_VERSION);
        boolean created = randomBoolean();
        boolean forcedRefresh = randomBoolean();
        Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfo = RandomObjects.randomShardInfo(random());
        IndexResponse actual = new IndexResponse(new ShardId(index, indexUUid, shardId), id, seqNo, primaryTerm, version, created);
        actual.setForcedRefresh(forcedRefresh);
        actual.setShardInfo(shardInfo.v1());

        return actual;
    }
}
