/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONAction;
import org.opensearch.geospatial.ip2geo.action.RestPutDatasourceHandler;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.geospatial.stats.upload.RestUploadStatsAction;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;

public class GeospatialPluginTests extends OpenSearchTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet(Ip2GeoSettings.settings()));
    private final List<RestHandler> SUPPORTED_REST_HANDLERS = List.of(
        new RestUploadGeoJSONAction(),
        new RestUploadStatsAction(),
        new RestPutDatasourceHandler(Settings.EMPTY, clusterSettings)
    );
    private final Client client;
    private final ClusterService clusterService;
    private final IngestService ingestService;

    public GeospatialPluginTests() {
        client = mock(Client.class);
        when(client.settings()).thenReturn(Settings.EMPTY);
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        ingestService = mock(IngestService.class);
        when(ingestService.getClusterService()).thenReturn(clusterService);
    }

    public void testIsAnIngestPlugin() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        assertTrue(plugin instanceof IngestPlugin);
    }

    public void testFeatureProcessorIsAdded() {
        Processor.Parameters parameters = new Processor.Parameters(
            mock(Environment.class),
            mock(ScriptService.class),
            null,
            null,
            null,
            null,
            ingestService,
            client,
            null
        );

        GeospatialPlugin plugin = new GeospatialPlugin();
        Map<String, Processor.Factory> processors = plugin.getProcessors(parameters);
        assertTrue(processors.containsKey(FeatureProcessor.TYPE));
        assertTrue(processors.get(FeatureProcessor.TYPE) instanceof FeatureProcessor.Factory);
    }

    public void testTotalRestHandlers() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        assertEquals(
            SUPPORTED_REST_HANDLERS.size(),
            plugin.getRestHandlers(Settings.EMPTY, null, clusterSettings, null, null, null, null).size()
        );
    }

    public void testUploadGeoJSONTransportIsAdded() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = plugin.getActions();
        assertEquals(1, actions.stream().filter(actionHandler -> actionHandler.getAction() instanceof UploadGeoJSONAction).count());
    }
}
