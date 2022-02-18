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

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.IndicesAdminClient;
import org.opensearch.test.OpenSearchTestCase;

public class IndexManagerTests extends OpenSearchTestCase {

    public static final int INDEX_PROPERTIES_SIZE = 4;
    private static final boolean FAIL = false;
    private static final boolean SUCCEED = true;
    private IndicesAdminClient mockClient;
    private IndexManager manager;
    private StepListener<Void> listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mockClient = mock(IndicesAdminClient.class);
        manager = new IndexManager(mockClient);
        listener = new StepListener<>();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    private Map<String, String> randomFieldMap(int size) {
        Map<String, String> fieldMap = new HashMap<>();
        IntStream.range(0, size).forEach(unUsed -> fieldMap.put(randomLowerCaseString(), randomLowerCaseString()));
        return fieldMap;
    }

    private void mockCreateAction(String indexName, boolean actionSucceeded) {
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            assert args.length == 2;
            CreateIndexRequest request = (CreateIndexRequest) args[0];
            assertEquals("index name did not match", indexName, request.index());
            ActionListener<CreateIndexResponse> createIndexAction = (ActionListener<CreateIndexResponse>) args[1];
            if (actionSucceeded) {
                // call onResponse flow
                createIndexAction.onResponse(null);
                return null;
            }
            createIndexAction.onFailure(new ResourceAlreadyExistsException(indexName));
            return null;
        }).when(mockClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
    }

    public void testIndexCreationSucceeded() {
        String indexName = randomLowerCaseString();
        mockCreateAction(indexName, SUCCEED);
        manager.create(indexName, randomFieldMap(INDEX_PROPERTIES_SIZE), listener);
        verify(mockClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
        assertNull("create index failed", listener.result());

    }

    public void testIndexCreationFailed() {
        String indexName = randomLowerCaseString();
        mockCreateAction(indexName, FAIL);
        manager.create(indexName, randomFieldMap(INDEX_PROPERTIES_SIZE), listener);
        verify(mockClient).create(any(CreateIndexRequest.class), any(ActionListener.class));
        expectThrows(ResourceAlreadyExistsException.class, listener::result);
    }
}
