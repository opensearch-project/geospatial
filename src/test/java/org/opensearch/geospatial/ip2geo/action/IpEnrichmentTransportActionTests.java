/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.geospatial.action.IpEnrichmentRequest;
import org.opensearch.geospatial.action.IpEnrichmentResponse;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.tasks.Task;

public class IpEnrichmentTransportActionTests extends Ip2GeoTestCase {

    private IpEnrichmentTransportAction action;

    @Mock
    Task task;

    @Mock
    ActionListener<ActionResponse> listener;

    @Mock
    Datasource mockDataSource;

    @Before
    public void init() {
        action = new IpEnrichmentTransportAction(transportService, actionFilters, ip2GeoCachedDao, datasourceDao);
    }

    /**
     * When dataSource is provided.
     */
    @Test
    public void testDoExecute_All_Succeed() {
        IpEnrichmentRequest request = new IpEnrichmentRequest("192.168.1.1", "testSource");
        action.doExecute(task, request, listener);

        verify(listener, times(1)).onResponse(any(IpEnrichmentResponse.class));
    }

    /**
     * When dataSource is absent, but default is valid.
     */
    @Test
    public void testDoExecute_WithDefaultDataSource() {
        when(mockDataSource.getName()).thenReturn("defaultDataSourceName");
        when(datasourceDao.getAllDatasources()).thenReturn(List.of(mockDataSource));
        when(ip2GeoCachedDao.getIndexName(eq("defaultDataSourceName"))).thenReturn("defaultIndexName");
        when(ip2GeoCachedDao.getGeoData(eq("defaultIndexName"), any())).thenReturn(Collections.emptyMap());

        IpEnrichmentRequest request = new IpEnrichmentRequest("192.168.1.1", null);
        action.doExecute(task, request, listener);
        verify(listener, times(1)).onResponse(any(IpEnrichmentResponse.class));
    }

    /**
     * No alternative dataSource, exception being thrown to indicate this.
     */
    @Test
    public void testDoExecute_WithNoAlternativeDataSource() {
        IpEnrichmentRequest request = new IpEnrichmentRequest("192.168.1.1", null);
        action.doExecute(task, request, listener);

        verify(listener, times(1)).onFailure(any(IllegalArgumentException.class));
    }

}
