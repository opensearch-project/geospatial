/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import org.opensearch.action.StepListener;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;

public class PipelineManagerTests extends OpenSearchTestCase {

    public static final boolean ACTION_SUCCESS = true;
    public static final boolean ACTION_FAILED = false;
    private ClusterAdminClient mockClient;
    private PipelineManager manager;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClient = mock(ClusterAdminClient.class);
        manager = new PipelineManager(mockClient);
    }

    private void mockCreatePipelineAction(boolean actionSucceeded) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            ActionListener<AcknowledgedResponse> createPipelineAction = (ActionListener<AcknowledgedResponse>) args[1];
            if (actionSucceeded) {             // call onResponse flow
                createPipelineAction.onResponse(new AcknowledgedResponse(true));
                return null;
            }
            createPipelineAction.onFailure(new IllegalStateException(randomLowerCaseString()));
            return null;
        }).when(mockClient).putPipeline(any(PutPipelineRequest.class), any(ActionListener.class));
    }

    private void mockDeletePipelineAction(boolean status) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            ActionListener<AcknowledgedResponse> createPipelineAction = (ActionListener<AcknowledgedResponse>) args[1];
            if (status) {
                createPipelineAction.onResponse(new AcknowledgedResponse(true));
                return null;
            }
            createPipelineAction.onFailure(new IllegalStateException(randomLowerCaseString()));
            return null;
        }).when(mockClient).deletePipeline(any(DeletePipelineRequest.class), any(ActionListener.class));
    }

    public void testCreatePipelineSuccess() {
        mockCreatePipelineAction(ACTION_SUCCESS);
        StepListener<String> createPipelineListener = new StepListener<>();
        manager.create(randomLowerCaseString(), createPipelineListener);
        verify(mockClient).putPipeline(any(PutPipelineRequest.class), any(ActionListener.class));
        assertNotNull("create pipeline failed", createPipelineListener.result());
    }

    public void testCreatePipelineFailed() {
        mockCreatePipelineAction(ACTION_FAILED);
        StepListener<String> createPipelineListener = new StepListener<>();
        manager.create(randomLowerCaseString(), createPipelineListener);
        verify(mockClient).putPipeline(any(PutPipelineRequest.class), any(ActionListener.class));
        expectThrows(IllegalStateException.class, createPipelineListener::result);
    }

    public void testDeletePipelineSuccess() {
        mockDeletePipelineAction(ACTION_SUCCESS);
        StepListener<Exception> deletePipelineListener = new StepListener<>();
        manager.delete(randomLowerCaseString(), deletePipelineListener, () -> null);
        verify(mockClient).deletePipeline(any(DeletePipelineRequest.class), any(ActionListener.class));
        assertNull("delete pipeline failed", deletePipelineListener.result());
    }

    public void testDeletePipelineFailed() {
        mockDeletePipelineAction(ACTION_FAILED);
        StepListener<Exception> deletePipelineListener = new StepListener<>();
        manager.delete(randomLowerCaseString(), deletePipelineListener, IllegalStateException::new);
        verify(mockClient).deletePipeline(any(DeletePipelineRequest.class), any(ActionListener.class));
        expectThrows(IllegalStateException.class, deletePipelineListener::result);
    }
}
