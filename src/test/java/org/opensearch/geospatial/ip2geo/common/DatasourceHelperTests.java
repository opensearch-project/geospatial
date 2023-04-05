/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geospatial.ip2geo.common;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.test.rest.RestActionTestCase;

public class DatasourceHelperTests extends RestActionTestCase {

    public void testUpdateDatasource() throws Exception {
        Instant previousTime = Instant.now().minusMillis(1);
        Datasource datasource = new Datasource(
            "testId",
            previousTime,
            null,
            false,
            null,
            null,
            DatasourceState.PREPARING,
            null,
            null,
            null
        );

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof IndexRequest);
            IndexRequest request = (IndexRequest) actionRequest;
            assertEquals(datasource.getId(), request.id());
            assertEquals(DocWriteRequest.OpType.INDEX, request.opType());
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            return null;
        });

        DatasourceHelper.updateDatasource(verifyingClient, datasource, TimeValue.timeValueSeconds(30));
        assertTrue(previousTime.isBefore(datasource.getLastUpdateTime()));
    }

    public void testGetDatasourceException() throws Exception {
        Datasource datasource = new Datasource(
            "testId",
            Instant.now(),
            null,
            false,
            new IntervalSchedule(Instant.now(), 1, ChronoUnit.DAYS),
            "https://test.com",
            DatasourceState.PREPARING,
            null,
            null,
            null
        );

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            assertTrue(actionRequest instanceof GetRequest);
            GetRequest request = (GetRequest) actionRequest;
            assertEquals(datasource.getId(), request.id());
            assertEquals(DatasourceExtension.JOB_INDEX_NAME, request.index());
            throw new IndexNotFoundException(DatasourceExtension.JOB_INDEX_NAME);
        });

        assertNull(DatasourceHelper.getDatasource(verifyingClient, datasource.getId(), TimeValue.timeValueSeconds(30)));
    }
}
