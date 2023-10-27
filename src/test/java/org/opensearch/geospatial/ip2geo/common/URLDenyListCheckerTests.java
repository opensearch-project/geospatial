/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.geospatial.ClusterSettingHelper;
import org.opensearch.node.Node;
import org.opensearch.test.OpenSearchTestCase;

public class URLDenyListCheckerTests extends OpenSearchTestCase {
    private ClusterSettingHelper clusterSettingHelper;

    @Before
    public void init() {
        clusterSettingHelper = new ClusterSettingHelper();
    }

    @SneakyThrows
    public void testToUrlIfNotInDenyListWithBlockedAddress() {
        Node mockNode = clusterSettingHelper.createMockNode(
            Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Arrays.asList("127.0.0.0/8"))
        );
        mockNode.start();
        try {
            ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
            URLDenyListChecker urlDenyListChecker = new URLDenyListChecker(clusterService.getClusterSettings());
            String endpoint = String.format(
                Locale.ROOT,
                "https://127.%d.%d.%d/v1/manifest.json",
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256)
            );
            expectThrows(IllegalArgumentException.class, () -> urlDenyListChecker.toUrlIfNotInDenyList(endpoint));
        } finally {
            mockNode.close();
        }
    }

    @SneakyThrows
    public void testToUrlIfNotInDenyListWithNonBlockedAddress() {
        Node mockNode = clusterSettingHelper.createMockNode(
            Map.of(Ip2GeoSettings.DATASOURCE_ENDPOINT_DENYLIST.getKey(), Arrays.asList("127.0.0.0/8"))
        );
        mockNode.start();
        try {
            ClusterService clusterService = mockNode.injector().getInstance(ClusterService.class);
            URLDenyListChecker urlDenyListChecker = new URLDenyListChecker(clusterService.getClusterSettings());
            String endpoint = String.format(
                Locale.ROOT,
                "https://128.%d.%d.%d/v1/manifest.json",
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256),
                Randomness.get().nextInt(256)
            );
            // Expect no exception
            urlDenyListChecker.toUrlIfNotInDenyList(endpoint);
        } finally {
            mockNode.close();
        }
    }
}
