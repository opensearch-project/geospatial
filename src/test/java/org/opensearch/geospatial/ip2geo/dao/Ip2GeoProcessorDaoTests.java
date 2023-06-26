/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.dao;

import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.processor.Ip2GeoProcessor;
import org.opensearch.ingest.IngestMetadata;
import org.opensearch.ingest.PipelineConfiguration;

public class Ip2GeoProcessorDaoTests extends Ip2GeoTestCase {
    private Ip2GeoProcessorDao ip2GeoProcessorDao;

    @Before
    public void init() {
        ip2GeoProcessorDao = new Ip2GeoProcessorDao(ingestService);
    }

    public void testGetProcessors_whenNullMetadata_thenReturnEmpty() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        when(metadata.custom(IngestMetadata.TYPE)).thenReturn(null);

        List<Ip2GeoProcessor> ip2GeoProcessorList = ip2GeoProcessorDao.getProcessors(datasourceName);
        assertTrue(ip2GeoProcessorList.isEmpty());
    }

    public void testGetProcessors_whenNoProcessorForGivenDatasource_thenReturnEmpty() {
        String datasourceBeingUsed = GeospatialTestHelper.randomLowerCaseString();
        String datasourceNotBeingUsed = GeospatialTestHelper.randomLowerCaseString();
        String pipelineId = GeospatialTestHelper.randomLowerCaseString();
        Map<String, PipelineConfiguration> pipelines = new HashMap<>();
        pipelines.put(pipelineId, createPipelineConfiguration());
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        when(metadata.custom(IngestMetadata.TYPE)).thenReturn(ingestMetadata);
        Ip2GeoProcessor ip2GeoProcessor = randomIp2GeoProcessor(datasourceBeingUsed);
        when(ingestService.getProcessorsInPipeline(pipelineId, Ip2GeoProcessor.class)).thenReturn(Arrays.asList(ip2GeoProcessor));

        List<Ip2GeoProcessor> ip2GeoProcessorList = ip2GeoProcessorDao.getProcessors(datasourceNotBeingUsed);
        assertTrue(ip2GeoProcessorList.isEmpty());
    }

    public void testGetProcessors_whenProcessorsForGivenDatasource_thenReturnProcessors() {
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        String pipelineId = GeospatialTestHelper.randomLowerCaseString();
        Map<String, PipelineConfiguration> pipelines = new HashMap<>();
        pipelines.put(pipelineId, createPipelineConfiguration());
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        when(metadata.custom(IngestMetadata.TYPE)).thenReturn(ingestMetadata);
        Ip2GeoProcessor ip2GeoProcessor = randomIp2GeoProcessor(datasourceName);
        when(ingestService.getProcessorsInPipeline(pipelineId, Ip2GeoProcessor.class)).thenReturn(Arrays.asList(ip2GeoProcessor));

        List<Ip2GeoProcessor> ip2GeoProcessorList = ip2GeoProcessorDao.getProcessors(datasourceName);
        assertEquals(1, ip2GeoProcessorList.size());
        assertEquals(ip2GeoProcessor.getDatasourceName(), ip2GeoProcessorList.get(0).getDatasourceName());
    }

    private PipelineConfiguration createPipelineConfiguration() {
        String id = GeospatialTestHelper.randomLowerCaseString();
        ByteBuffer byteBuffer = ByteBuffer.wrap(GeospatialTestHelper.randomLowerCaseString().getBytes(StandardCharsets.US_ASCII));
        BytesReference config = BytesReference.fromByteBuffer(byteBuffer);
        return new PipelineConfiguration(id, config, XContentType.JSON);
    }
}
