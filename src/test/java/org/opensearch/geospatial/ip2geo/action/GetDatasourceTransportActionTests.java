/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.tasks.Task;

public class GetDatasourceTransportActionTests extends Ip2GeoTestCase {
    private GetDatasourceTransportAction action;

    @Before
    public void init() {
        action = new GetDatasourceTransportAction(transportService, actionFilters, datasourceFacade);
    }

    public void testDoExecute_whenAll_thenSucceed() throws Exception {
        Task task = mock(Task.class);
        GetDatasourceRequest request = new GetDatasourceRequest(new String[] { "_all" });
        ActionListener<GetDatasourceResponse> listener = mock(ActionListener.class);

        // Run
        action.doExecute(task, request, listener);

        // Verify
        ArgumentCaptor<ActionListener<List<Datasource>>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceFacade).getAllDatasources(captor.capture());

        // Run
        List<Datasource> datasources = Arrays.asList(randomDatasource(), randomDatasource());
        captor.getValue().onResponse(datasources);

        // Verify
        verify(listener).onResponse(new GetDatasourceResponse(datasources));

        // Run
        RuntimeException exception = new RuntimeException();
        captor.getValue().onFailure(exception);

        // Verify
        verify(listener).onFailure(exception);
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
        ArgumentCaptor<ActionListener<List<Datasource>>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceFacade).getDatasources(eq(datasourceNames), captor.capture());

        // Run
        captor.getValue().onResponse(datasources);

        // Verify
        verify(listener).onResponse(new GetDatasourceResponse(datasources));

        // Run
        RuntimeException exception = new RuntimeException();
        captor.getValue().onFailure(exception);

        // Verify
        verify(listener).onFailure(exception);
    }
}
