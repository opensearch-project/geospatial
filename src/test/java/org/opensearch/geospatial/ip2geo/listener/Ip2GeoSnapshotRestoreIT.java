/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.listener;

import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.WarningsHandler;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoDataServer;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceRequest;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.geospatial.ip2geo.processor.Ip2GeoProcessor;

import lombok.SneakyThrows;

public class Ip2GeoSnapshotRestoreIT extends GeospatialRestTestCase {
    private static final String PREFIX = Ip2GeoSnapshotRestoreIT.class.getSimpleName().toLowerCase(Locale.ROOT);
    private static final String REPO_NAME = PREFIX + "-repo";
    private static final String SNAPSHOT_NAME = PREFIX + "-snapshot";
    private static final String IP_FIELD = "ip";
    private static final String TARGET_FIELD = "geo";
    private static final String SEATTLE_IP = "10.0.0.1";
    private static final String CITY = "city";
    private static final RequestOptions PERMISSIVE = permissiveOptions();

    private static RequestOptions permissiveOptions() {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.setWarningsHandler(WarningsHandler.PERMISSIVE);
        return builder.build();
    }

    @BeforeClass
    public static void start() {
        Ip2GeoDataServer.start();
    }

    @AfterClass
    public static void stop() {
        Ip2GeoDataServer.stop();
    }

    @SneakyThrows
    public void testSnapshotRestore_whenJobAndDataIndicesRestored_thenDatasourceIsRefreshed() {
        // Needs path.repo on the cluster and permission to delete and restore ip2geo system indices,
        // which the security-enabled test cluster does not provide
        assumeFalse("snapshot restore test is not supported on security enabled cluster", Boolean.getBoolean("https"));
        updateClusterSetting(Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Collections.emptyList()));
        String datasourceName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        String pipelineName = PREFIX + GeospatialTestHelper.randomLowerCaseString();

        // Create datasource and pipeline, and verify enrichment works
        createDatasource(
            datasourceName,
            Map.of(PutDatasourceRequest.ENDPOINT_FIELD.getPreferredName(), Ip2GeoDataServer.getEndpointCity())
        );
        waitForDatasourceToBeAvailable(datasourceName, Duration.ofSeconds(30));
        createIp2GeoPipeline(pipelineName, datasourceName);
        assertEquals("Seattle", lookupCity(pipelineName, SEATTLE_IP));
        Set<String> originalDataIndices = getDataIndices();
        assertEquals(1, originalDataIndices.size());
        long originalSucceededAt = getLastSucceededAt(datasourceName);

        // Take a snapshot of job index and data indices
        String indices = DatasourceExtension.JOB_INDEX_NAME + "," + IP2GEO_DATA_INDEX_NAME_PREFIX + "*";
        createRepository();
        Request snapshot = new Request("PUT", "/_snapshot/" + REPO_NAME + "/" + SNAPSHOT_NAME);
        snapshot.addParameter("wait_for_completion", "true");
        snapshot.setJsonEntity(String.format(Locale.ROOT, "{\"indices\":\"%s\",\"include_global_state\":false}", indices));
        snapshot.setOptions(PERMISSIVE);
        String snapshotResponse = EntityUtils.toString(client().performRequest(snapshot).getEntity());
        assertTrue(snapshotResponse, snapshotResponse.contains("\"state\":\"SUCCESS\""));

        // Remove everything: pipeline, datasource (drops data index) and the job index itself
        deletePipeline(pipelineName);
        deleteDatasource(datasourceName, 3);
        Request deleteJobIndex = new Request("DELETE", "/" + DatasourceExtension.JOB_INDEX_NAME);
        deleteJobIndex.setOptions(PERMISSIVE);
        client().performRequest(deleteJobIndex);
        assertTrue(getDataIndices().isEmpty());

        // Restore
        Instant restoreTime = Instant.now();
        Request restore = new Request("POST", "/_snapshot/" + REPO_NAME + "/" + SNAPSHOT_NAME + "/_restore");
        restore.addParameter("wait_for_completion", "true");
        restore.setJsonEntity(String.format(Locale.ROOT, "{\"indices\":\"%s\",\"include_global_state\":false}", indices));
        restore.setOptions(PERMISSIVE);
        client().performRequest(restore);

        // Datasource metadata is back
        assertNotNull(getDatasource(datasourceName));

        // Restored data index is deleted, and a fresh data index is built by the forced update
        Instant deadline = Instant.now().plus(Duration.ofMinutes(3));
        Set<String> dataIndices = getDataIndices();
        long succeededAt = getLastSucceededAt(datasourceName);
        while ((dataIndices.size() != 1 || originalDataIndices.equals(dataIndices) || succeededAt <= restoreTime.toEpochMilli())
            && Instant.now().isBefore(deadline)) {
            Thread.sleep(1000);
            dataIndices = getDataIndices();
            succeededAt = getLastSucceededAt(datasourceName);
        }
        logger.info(
            "original indices {}, after restore {}, succeededAt {} -> {}",
            originalDataIndices,
            dataIndices,
            originalSucceededAt,
            succeededAt
        );
        assertEquals(1, dataIndices.size());
        assertNotEquals(originalDataIndices, dataIndices);
        assertTrue(succeededAt > restoreTime.toEpochMilli());
        waitForDatasourceToBeAvailable(datasourceName, Duration.ofSeconds(10));

        // Processor works with the restored datasource
        createIp2GeoPipeline(pipelineName, datasourceName);
        assertEquals("Seattle", lookupCity(pipelineName, SEATTLE_IP));

        deletePipeline(pipelineName);
        deleteDatasource(datasourceName, 3);
    }

    @SneakyThrows
    private void createRepository() {
        Request request = new Request("PUT", "/_snapshot/" + REPO_NAME);
        request.setJsonEntity(String.format(Locale.ROOT, "{\"type\":\"fs\",\"settings\":{\"location\":\"%s\"}}", REPO_NAME));
        client().performRequest(request);
    }

    @SneakyThrows
    private Set<String> getDataIndices() {
        Request request = new Request("GET", "/_cat/indices/" + IP2GEO_DATA_INDEX_NAME_PREFIX + "*");
        request.addParameter("h", "index");
        request.addParameter("expand_wildcards", "all");
        request.setOptions(PERMISSIVE);
        String body = EntityUtils.toString(client().performRequest(request).getEntity()).trim();
        return body.isEmpty() ? Set.of() : Arrays.stream(body.split("\\s+")).collect(Collectors.toSet());
    }

    @SneakyThrows
    private long getLastSucceededAt(final String datasourceName) {
        List<Map<String, Object>> datasources = (List<Map<String, Object>>) getDatasource(datasourceName).get("datasources");
        Map<String, Object> stats = (Map<String, Object>) datasources.get(0).get("update_stats");
        Object value = stats.get("last_succeeded_at_in_epoch_millis");
        return value == null ? 0 : ((Number) value).longValue();
    }

    @SneakyThrows
    private void createIp2GeoPipeline(final String pipelineName, final String datasourceName) {
        Map<String, Object> processorConfig = buildProcessorConfig(
            Ip2GeoProcessor.TYPE,
            Map.of(
                Ip2GeoProcessor.CONFIG_FIELD,
                IP_FIELD,
                Ip2GeoProcessor.CONFIG_DATASOURCE,
                datasourceName,
                Ip2GeoProcessor.CONFIG_TARGET_FIELD,
                TARGET_FIELD,
                Ip2GeoProcessor.CONFIG_PROPERTIES,
                List.of(CITY)
            )
        );
        createPipeline(pipelineName, Optional.empty(), List.of(processorConfig));
    }

    @SneakyThrows
    private String lookupCity(final String pipelineName, final String ip) {
        Map<String, Object> response = simulatePipeline(pipelineName, List.of(Map.of(SOURCE, Map.of(IP_FIELD, ip))));
        List<Map<String, Map<String, Object>>> docs = (List<Map<String, Map<String, Object>>>) response.get("docs");
        Map<String, Object> source = (Map<String, Object>) docs.get(0).get("doc").get(SOURCE);
        Map<String, Object> geo = (Map<String, Object>) source.get(TARGET_FIELD);
        return geo == null ? null : (String) geo.get(CITY);
    }
}
