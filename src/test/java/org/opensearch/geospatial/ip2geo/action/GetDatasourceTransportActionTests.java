/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.tasks.Task;

public class GetDatasourceTransportActionTests extends Ip2GeoTestCase {
    private GetDatasourceTransportAction action;

    @Before
    public void init() {
        action = new GetDatasourceTransportAction(transportService, actionFilters, datasourceDao);
    }

    public void testDoExecute_whenAll_thenSucceed() {
        Task task = mock(Task.class);
        GetDatasourceRequest request = new GetDatasourceRequest(new String[] { "_all" });
        ActionListener<GetDatasourceResponse> listener = mock(ActionListener.class);

        // Run
        action.doExecute(task, request, listener);

        // Verify
        verify(datasourceDao).getAllDatasources(any(ActionListener.class));
    }

    public void testDoExecute_whenNames_thenSucceed() {
        Task task = mock(Task.class);
        List<Datasource> datasources = Arrays.asList(randomDatasource(), randomDatasource());
        String[] datasourceNames = datasources.stream().map(Datasource::getName).toArray(String[]::new);

        GetDatasourceRequest request = new GetDatasourceRequest(datasourceNames);
        ActionListener<GetDatasourceResponse> listener = mock(ActionListener.class);

        // Run
        action.doExecute(task, request, listener);

        // Verify
        verify(datasourceDao).getDatasources(eq(datasourceNames), any(ActionListener.class));
    }

    public void testDoExecute_whenNull_thenException() {
        Task task = mock(Task.class);
        GetDatasourceRequest request = new GetDatasourceRequest((String[]) null);
        ActionListener<GetDatasourceResponse> listener = mock(ActionListener.class);

        // Run
        Exception exception = expectThrows(OpenSearchException.class, () -> action.doExecute(task, request, listener));

        // Verify
        assertTrue(exception.getMessage().contains("should not be null"));
    }

    public void testNewActionListener_whenOnResponse_thenSucceed() {
        List<Datasource> datasources = Arrays.asList(randomDatasource(), randomDatasource());
        ActionListener<GetDatasourceResponse> actionListener = mock(ActionListener.class);

        // Run
        action.newActionListener(actionListener).onResponse(datasources);

        // Verify
        verify(actionListener).onResponse(new GetDatasourceResponse(datasources));
    }

    public void testNewActionListener_whenOnFailureWithNoSuchIndexException_thenEmptyDatasource() {
        ActionListener<GetDatasourceResponse> actionListener = mock(ActionListener.class);

        // Run
        action.newActionListener(actionListener).onFailure(new IndexNotFoundException("no index"));

        // Verify
        verify(actionListener).onResponse(new GetDatasourceResponse(Collections.emptyList()));
    }

    public void testNewActionListener_whenOnFailure_thenFails() {
        ActionListener<GetDatasourceResponse> actionListener = mock(ActionListener.class);

        // Run
        action.newActionListener(actionListener).onFailure(new RuntimeException());

        // Verify
        verify(actionListener).onFailure(any(RuntimeException.class));
    }
}
