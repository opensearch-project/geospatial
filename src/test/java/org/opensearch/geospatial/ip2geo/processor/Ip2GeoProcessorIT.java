/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.processor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.SneakyThrows;

import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.GeospatialTestHelper;

public class Ip2GeoProcessorIT extends GeospatialRestTestCase {

    @SneakyThrows
    public void testCreateIp2GeoProcessor_whenNoSuchDatasourceExist_thenFails() {
        String pipelineName = GeospatialTestHelper.randomLowerCaseString();

        // Run
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> createIp2GeoProcessorPipeline(pipelineName, Collections.emptyMap())
        );

        // Verify
        assertTrue(exception.getMessage().contains("doesn't exist"));
    }

    private Response createIp2GeoProcessorPipeline(final String pipelineName, final Map<String, String> properties) throws IOException {
        String field = GeospatialTestHelper.randomLowerCaseString();
        String datasourceName = GeospatialTestHelper.randomLowerCaseString();
        Map<String, String> defaultProperties = Map.of(
            Ip2GeoProcessor.CONFIG_FIELD,
            field,
            Ip2GeoProcessor.CONFIG_DATASOURCE,
            datasourceName
        );
        Map<String, String> baseProperties = new HashMap<>();
        baseProperties.putAll(defaultProperties);
        baseProperties.putAll(properties);
        Map<String, Object> processorConfig = buildProcessorConfig(Ip2GeoProcessor.TYPE, baseProperties);

        return createPipeline(pipelineName, Optional.empty(), Arrays.asList(processorConfig));
    }
}
