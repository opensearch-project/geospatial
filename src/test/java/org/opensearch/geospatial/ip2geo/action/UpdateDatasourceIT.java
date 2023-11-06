/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.action;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import lombok.SneakyThrows;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.geospatial.GeospatialRestTestCase;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoDataServer;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;

public class UpdateDatasourceIT extends GeospatialRestTestCase {
    // Use this value in resource name to avoid name conflict among tests
    private static final String PREFIX = UpdateDatasourceIT.class.getSimpleName().toLowerCase(Locale.ROOT);

    @BeforeClass
    public static void start() {
        Ip2GeoDataServer.start();
    }

    @AfterClass
    public static void stop() {
        Ip2GeoDataServer.stop();
    }

    @SneakyThrows
    public void testUpdateDatasource_whenPrivateNetwork_thenBlocked() {
        // Reset deny list to allow private network access during test
        updateClusterSetting(Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Collections.emptyList()));

        boolean isDatasourceCreated = false;
        String datasourceName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        try {
            Map<String, Object> datasourceProperties = Map.of(
                PutDatasourceRequest.ENDPOINT_FIELD.getPreferredName(),
                Ip2GeoDataServer.getEndpointCountry()
            );

            // Create datasource and wait for it to be available
            createDatasource(datasourceName, datasourceProperties);
            isDatasourceCreated = true;
            waitForDatasourceToBeAvailable(datasourceName, Duration.ofSeconds(10));

            // Revert deny list to its default value and private network ip should be blocked
            updateClusterSetting(
                Map.of(
                    Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(),
                    Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.get(Settings.EMPTY)
                )
            );
            int updateIntervalInDays = 1;
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> updateDatasourceEndpoint(datasourceName, "http://127.0.0.1:9200/city/manifest_local.json", updateIntervalInDays)
            );
            assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
            assertTrue(exception.getMessage().contains("blocked by deny list"));
        } finally {
            if (isDatasourceCreated) {
                deleteDatasource(datasourceName, 3);
            }
        }
    }

    @SneakyThrows
    public void testUpdateDatasource_whenValidInput_thenUpdated() {
        // Reset deny list to allow private network access during test
        updateClusterSetting(Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Collections.emptyList()));

        boolean isDatasourceCreated = false;
        String datasourceName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        try {
            Map<String, Object> datasourceProperties = Map.of(
                PutDatasourceRequest.ENDPOINT_FIELD.getPreferredName(),
                Ip2GeoDataServer.getEndpointCountry()
            );

            // Create datasource and wait for it to be available
            createDatasource(datasourceName, datasourceProperties);
            isDatasourceCreated = true;
            waitForDatasourceToBeAvailable(datasourceName, Duration.ofSeconds(10));

            int updateIntervalInDays = 1;
            updateDatasourceEndpoint(datasourceName, Ip2GeoDataServer.getEndpointCity(), updateIntervalInDays);
            List<Map<String, Object>> datasources = (List<Map<String, Object>>) getDatasource(datasourceName).get("datasources");

            assertEquals(Ip2GeoDataServer.getEndpointCity(), datasources.get(0).get("endpoint"));
            assertEquals(updateIntervalInDays, datasources.get(0).get("update_interval_in_days"));
        } finally {
            if (isDatasourceCreated) {
                deleteDatasource(datasourceName, 3);
            }
        }
    }

    @SneakyThrows
    public void testUpdateDatasource_whenIncompatibleFields_thenFails() {
        // Reset deny list to allow private network access during test
        updateClusterSetting(Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Collections.emptyList()));

        boolean isDatasourceCreated = false;
        String datasourceName = PREFIX + GeospatialTestHelper.randomLowerCaseString();
        try {
            Map<String, Object> datasourceProperties = Map.of(
                PutDatasourceRequest.ENDPOINT_FIELD.getPreferredName(),
                Ip2GeoDataServer.getEndpointCity()
            );

            // Create datasource and wait for it to be available
            createDatasource(datasourceName, datasourceProperties);
            isDatasourceCreated = true;
            waitForDatasourceToBeAvailable(datasourceName, Duration.ofSeconds(10));

            // Update should fail as country data does not have every fields that city data has
            int updateIntervalInDays = 1;
            ResponseException exception = expectThrows(
                ResponseException.class,
                () -> updateDatasourceEndpoint(datasourceName, Ip2GeoDataServer.getEndpointCountry(), updateIntervalInDays)
            );
            assertEquals(RestStatus.BAD_REQUEST.getStatus(), exception.getResponse().getStatusLine().getStatusCode());
        } finally {
            if (isDatasourceCreated) {
                deleteDatasource(datasourceName, 3);
            }
        }
    }

    private void updateDatasourceEndpoint(final String datasourceName, final String endpoint, final int updateInterval) throws IOException {
        Map<String, Object> properties = Map.of(
            UpdateDatasourceRequest.ENDPOINT_FIELD.getPreferredName(),
            endpoint,
            UpdateDatasourceRequest.UPDATE_INTERVAL_IN_DAYS_FIELD.getPreferredName(),
            updateInterval
        );
        updateDatasource(datasourceName, properties);
    }
}
