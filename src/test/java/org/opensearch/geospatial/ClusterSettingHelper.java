/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import static org.apache.lucene.tests.util.LuceneTestCase.createTempDir;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.OpenSearchTestCase.getTestTransportPlugin;
import static org.opensearch.test.OpenSearchTestCase.getTestTransportType;
import static org.opensearch.test.OpenSearchTestCase.randomLong;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.node.MockNode;
import org.opensearch.node.Node;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.MockHttpTransport;

@SuppressForbidden(reason = "used only for testing")
public class ClusterSettingHelper {
    public Node createMockNode(Map<String, Object> configSettings) throws IOException {
        Path configDir = createTempDir();
        File configFile = configDir.resolve("opensearch.yml").toFile();
        FileWriter configFileWriter = new FileWriter(configFile);

        for (Map.Entry<String, Object> setting : configSettings.entrySet()) {
            configFileWriter.write("\"" + setting.getKey() + "\": " + setting.getValue());
        }
        configFileWriter.close();
        return new MockNode(baseSettings().build(), basePlugins(), configDir, true);
    }

    private Collection<PluginInfo> basePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>();
        plugins.add(getTestTransportPlugin());
        plugins.add(MockHttpTransport.TestPlugin.class);
        plugins.add(TestGeospatialPlugin.class);

        Collection<PluginInfo> pluginInfos = plugins.stream()
            .map(
                p -> new PluginInfo(
                    p.getName(),
                    "classpath plugin",
                    "NA",
                    Version.CURRENT,
                    "1.8",
                    p.getName(),
                    null,
                    Collections.emptyList(),
                    false
                )
            )
            .collect(Collectors.toList());
        return pluginInfos;
    }

    private static Settings.Builder baseSettings() {
        final Path tempDir = createTempDir();
        return Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), InternalTestCluster.clusterName("single-node-cluster", randomLong()))
            .put(Environment.PATH_HOME_SETTING.getKey(), tempDir)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType())
            .put(dataNode());
    }
}
