/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.action.upload.geojson;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;

import java.util.Map;
import java.util.Optional;

import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.Client;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpClient;

public class ContentBuilderTests extends OpenSearchTestCase {

    public static final int MAX_NUM_ACTION = 5;
    public static final int MAX_FEATURES_COUNT = 3;
    public static final int ZERO_ACTIONS = 0;
    public static final int ZERO_FEATURES = 0;
    private Client mockClient;
    private NoOpClient noOpClient;
    private ContentBuilder contentBuilder;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        noOpClient = new NoOpClient(getTestName());
        mockClient = mock(Client.class);
        contentBuilder = new ContentBuilder(mockClient);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        noOpClient.close();
    }

    private BulkRequestBuilder mockBulkRequestBuilder(int noOfActions) {
        // mock BulkRequest
        BulkRequestBuilder mockBulkRequestBuilder = mock(BulkRequestBuilder.class);
        when(mockClient.prepareBulk()).thenReturn(mockBulkRequestBuilder);
        when(mockBulkRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)).thenReturn(mockBulkRequestBuilder);
        when(mockBulkRequestBuilder.add(any(IndexRequestBuilder.class))).thenReturn(null);
        when(mockBulkRequestBuilder.numberOfActions()).thenReturn(noOfActions);

        IndexRequestBuilder indexRequestBuilder = new IndexRequestBuilder(noOpClient, IndexAction.INSTANCE);
        when(mockClient.prepareIndex()).thenReturn(indexRequestBuilder);
        return mockBulkRequestBuilder;
    }

    public void testContentBuilderSuccess() {
        Map<String, Object> contentMap = GeospatialTestHelper.buildRequestContent(MAX_FEATURES_COUNT);
        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(contentMap);
        BulkRequestBuilder mockBulkRequestBuilder = mockBulkRequestBuilder(MAX_NUM_ACTION);
        final Optional<BulkRequestBuilder> prepare = contentBuilder.prepare(content, randomLowerCaseString());
        verify(mockClient).prepareBulk();
        verify(mockClient, times(MAX_FEATURES_COUNT)).prepareIndex();
        verify(mockBulkRequestBuilder, times(MAX_FEATURES_COUNT)).add(any(IndexRequestBuilder.class));
        assertTrue("failed to build request", prepare.isPresent());
    }

    public void testContentBuilderFailed() {
        Map<String, Object> contentMap = GeospatialTestHelper.buildRequestContent(ZERO_FEATURES);
        UploadGeoJSONRequestContent content = UploadGeoJSONRequestContent.create(contentMap);
        BulkRequestBuilder mockBulkRequestBuilder = mockBulkRequestBuilder(ZERO_ACTIONS);
        final Optional<BulkRequestBuilder> prepare = contentBuilder.prepare(content, randomLowerCaseString());
        verify(mockClient, never()).prepareBulk();
        verify(mockClient, never()).prepareIndex();
        verify(mockBulkRequestBuilder, never()).add(any(IndexRequestBuilder.class));
        assertFalse("Feature count should be empty", prepare.isPresent());
    }

}
