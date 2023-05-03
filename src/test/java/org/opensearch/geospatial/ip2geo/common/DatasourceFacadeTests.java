/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;

public class DatasourceFacadeTests extends Ip2GeoTestCase {
    private DatasourceFacade datasourceFacade;

    @Before
    public void init() {
        datasourceFacade = new DatasourceFacade(
            verifyingClient,
            new ClusterSettings(Settings.EMPTY, new HashSet<>(Ip2GeoSettings.settings()))
        );
    }

    public void testUpdateDatasource() throws Exception {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Datasource datasource = new Datasource(
            datasourceName,
            new IntervalSchedule(Instant.now().truncatedTo(ChronoUnit.MILLIS), 1, ChronoUnit.DAYS),
            "https://test.com"
        );
        Instant previousTime = Instant.now().minusMillis(1);
        datasource.setLastUpdateTime(previousTime);

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof IndexRequest);
            IndexRequest request = (IndexRequest) actionRequest;
            assertEquals(datasource.getName(), request.id());
            assertEquals(DocWriteRequest.OpType.INDEX, request.opType());
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            return null;
        });

        datasourceFacade.updateDatasource(datasource);
        assertTrue(previousTime.isBefore(datasource.getLastUpdateTime()));
    }

    public void testGetDatasourceException() throws Exception {
        Datasource datasource = setupClientForGetRequest(true, new IndexNotFoundException(DatasourceExtension.JOB_INDEX_NAME));
        assertNull(datasourceFacade.getDatasource(datasource.getName()));
    }

    public void testGetDatasourceExist() throws Exception {
        Datasource datasource = setupClientForGetRequest(true, null);
        assertEquals(datasource, datasourceFacade.getDatasource(datasource.getName()));
    }

    public void testGetDatasourceNotExist() throws Exception {
        Datasource datasource = setupClientForGetRequest(false, null);
        assertNull(datasourceFacade.getDatasource(datasource.getName()));
    }

    public void testGetDatasourceExistWithListener() {
        Datasource datasource = setupClientForGetRequest(true, null);
        ActionListener<Datasource> listener = mock(ActionListener.class);
        datasourceFacade.getDatasource(datasource.getName(), listener);
        verify(listener).onResponse(eq(datasource));
    }

    public void testGetDatasourceNotExistWithListener() {
        Datasource datasource = setupClientForGetRequest(false, null);
        ActionListener<Datasource> listener = mock(ActionListener.class);
        datasourceFacade.getDatasource(datasource.getName(), listener);
        verify(listener).onResponse(null);
    }

    private Datasource setupClientForGetRequest(final boolean isExist, final RuntimeException exception) {
        Datasource datasource = randomDatasource();

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof GetRequest);
            GetRequest request = (GetRequest) actionRequest;
            assertEquals(datasource.getName(), request.id());
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            GetResponse response = getMockedGetResponse(isExist ? datasource : null);
            if (exception != null) {
                throw exception;
            }
            return response;
        });
        return datasource;
    }

    private GetResponse getMockedGetResponse(Datasource datasource) {
        GetResponse response = mock(GetResponse.class);
        when(response.isExists()).thenReturn(datasource != null);
        when(response.getSourceAsBytesRef()).thenReturn(toBytesReference(datasource));
        return response;
    }

    private BytesReference toBytesReference(Datasource datasource) {
        if (datasource == null) {
            return null;
        }

        try {
            return BytesReference.bytes(datasource.toXContent(JsonXContent.contentBuilder(), null));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
