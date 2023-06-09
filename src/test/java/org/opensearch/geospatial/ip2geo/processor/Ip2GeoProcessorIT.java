/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.processor;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.Randomness;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoDataServer;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceRequest;
import org.opensearch.rest.RestStatus;

public class Ip2GeoProcessorIT extends GeospatialRestTestCase {
    // Use this value in resource name to avoid name conflict among tests
    private static final String PREFIX = Ip2GeoProcessorIT.class.getSimpleName().toLowerCase(Locale.ROOT);
    private static final String CITY = "city";
    private static final String COUNTRY = "country";
    private static final String IP = "ip";
    private static final String SOURCE = "_source";

    @SneakyThrows
    public void testCreateIp2GeoProcessor_whenNoSuchDatasourceExist_thenFails() {
        String pipelineName = PREFIX + GeospatialTestHelper.randomLowerCaseString();

        // Run
        ResponseException exception = expectThrows(
            ResponseException.class,
            () -> createIp2GeoProcessorPipeline(pipelineName, Collections.emptyMap())
        );

        // Verify
        assertTrue(exception.getMessage().contains("doesn't exist"));
        assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
    }

    @SneakyThrows
    public void testCreateIp2GeoProcessor_whenValidInput_thenAddData() {
        Ip2GeoDataServer.start();
        boolean isDatasourceCreated = false;
        boolean isProcessorCreated = false;
        String pipelineName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        String datasourceName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        try {
            String targetField = GeospatialTestHelper.randomLowerCaseString();
            String field = GeospatialTestHelper.randomLowerCaseString();

            Map<String, Object> datasourceProperties = Map.of(
                PutDatasourceRequest.ENDPOINT_FIELD.getPreferredName(),
                Ip2GeoDataServer.getEndpointCity()
            );

            // Create datasource and wait for it to be available
            createDatasource(datasourceName, datasourceProperties);
            isDatasourceCreated = true;
            // Creation of datasource with same name should fail
            ResponseException createException = expectThrows(
                ResponseException.class,
                () -> createDatasource(datasourceName, datasourceProperties)
            );
            // Verify
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), createException.getResponse().getStatusLine().getStatusCode());
            waitForDatasourceToBeAvailable(datasourceName, Duration.ofSeconds(10));

            Map<String, Object> processorProperties = Map.of(
                Ip2GeoProcessor.CONFIG_FIELD,
                field,
                Ip2GeoProcessor.CONFIG_DATASOURCE,
                datasourceName,
                Ip2GeoProcessor.CONFIG_TARGET_FIELD,
                targetField,
                Ip2GeoProcessor.CONFIG_PROPERTIES,
                Arrays.asList(CITY)
            );

            // Create ip2geo processor
            createIp2GeoProcessorPipeline(pipelineName, processorProperties);
            isProcessorCreated = true;

            Map<String, Map<String, String>> sampleData = getSampleData();
            List<Object> docs = sampleData.entrySet()
                .stream()
                .map(entry -> createDocument(field, entry.getKey()))
                .collect(Collectors.toList());

            // Simulate processor
            Map<String, Object> response = simulatePipeline(pipelineName, docs);

            // Verify data added to document
            List<Map<String, String>> sources = convertToListOfSources(response, targetField);
            sources.stream().allMatch(source -> source.size() == 1);
            List<String> cities = sources.stream().map(value -> value.get(CITY)).collect(Collectors.toList());
            List<String> expectedCities = sampleData.values().stream().map(value -> value.get(CITY)).collect(Collectors.toList());
            assertEquals(expectedCities, cities);

            // Delete datasource fails when there is a process using it
            ResponseException deleteException = expectThrows(ResponseException.class, () -> deleteDatasource(datasourceName));
            // Verify
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), deleteException.getResponse().getStatusLine().getStatusCode());
        } finally {
            Exception exception = null;
            try {
                if (isProcessorCreated) {
                    deletePipeline(pipelineName);
                }
                if (isDatasourceCreated) {
                    deleteDatasource(datasourceName, 3);
                }
            } catch (Exception e) {
                exception = e;
            }
            Ip2GeoDataServer.stop();
            if (exception != null) {
                throw exception;
            }
        }
    }

    private Response createIp2GeoProcessorPipeline(final String pipelineName, final Map<String, Object> properties) throws IOException {
        String field = GeospatialTestHelper.randomLowerCaseString();
        String datasourceName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        Map<String, Object> defaultProperties = Map.of(
            Ip2GeoProcessor.CONFIG_FIELD,
            field,
            Ip2GeoProcessor.CONFIG_DATASOURCE,
            datasourceName
        );
        Map<String, Object> baseProperties = new HashMap<>();
        baseProperties.putAll(defaultProperties);
        baseProperties.putAll(properties);
        Map<String, Object> processorConfig = buildProcessorConfig(Ip2GeoProcessor.TYPE, baseProperties);

        return createPipeline(pipelineName, Optional.empty(), Arrays.asList(processorConfig));
    }

    private Map<String, Map<String, String>> getSampleData() {
        Map<String, Map<String, String>> sampleData = new HashMap<>();
        sampleData.put(
            String.format(
                Locale.ROOT,
                "10.%d.%d.%d",
                Randomness.get().nextInt(255),
                Randomness.get().nextInt(255),
                Randomness.get().nextInt(255)
            ),
            Map.of(CITY, "Seattle", COUNTRY, "USA")
        );
        sampleData.put(
            String.format(
                Locale.ROOT,
                "127.%d.%d.%d",
                Randomness.get().nextInt(15),
                Randomness.get().nextInt(255),
                Randomness.get().nextInt(255)
            ),
            Map.of(CITY, "Vancouver", COUNTRY, "Canada")
        );
        sampleData.put(
            String.format(
                Locale.ROOT,
                "fd12:2345:6789:1:%x:%x:%x:%x",
                Randomness.get().nextInt(65535),
                Randomness.get().nextInt(65535),
                Randomness.get().nextInt(65535),
                Randomness.get().nextInt(65535)
            ),
            Map.of(CITY, "Bengaluru", COUNTRY, "India")
        );
        return sampleData;
    }

    private Map<String, Map<String, String>> createDocument(String... args) {
        if (args.length % 2 == 1) {
            throw new RuntimeException("Number of arguments should be even");
        }
        Map<String, String> source = new HashMap<>();
        for (int i = 0; i < args.length; i += 2) {
            source.put(args[0], args[1]);
        }
        return Map.of(SOURCE, source);
    }

    /**
     * This method convert returned value of simulatePipeline method to a list of sources
     *
     * For example,
     * Input:
     * {
     *   "docs" : [
     *     {
     *       "doc" : {
     *         "_index" : "_index",
     *         "_id" : "_id",
     *         "_source" : {
     *           "ip2geo" : {
     *             "ip" : "127.0.0.1",
     *             "city" : "Seattle"
     *           },
     *           "_ip" : "127.0.0.1"
     *         },
     *         "_ingest" : {
     *           "timestamp" : "2023-05-12T17:41:42.939703Z"
     *         }
     *       }
     *     }
     *   ]
     * }
     *
     * Output:
     * [
     *   {
     *     "ip" : "127.0.0.1",
     *     "city" : "Seattle"
     *   }
     * ]
     *
     */
    private List<Map<String, String>> convertToListOfSources(final Map<String, Object> data, final String targetField) {
        List<Map<String, Map<String, Object>>> docs = (List<Map<String, Map<String, Object>>>) data.get("docs");
        return docs.stream()
            .map(doc -> (Map<String, Map<String, String>>) doc.get("doc").get(SOURCE))
            .map(source -> source.get(targetField))
            .collect(Collectors.toList());
    }
}
