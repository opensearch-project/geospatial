/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import java.util.List;
import java.util.Map;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONAction;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.geospatial.stats.RestStatsAction;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.rest.RestHandler;
import org.opensearch.test.OpenSearchTestCase;

public class GeospatialPluginTests extends OpenSearchTestCase {

    private final List<RestHandler> SUPPORTED_REST_HANDLERS = List.of(new RestUploadGeoJSONAction(), new RestStatsAction());

    public void testIsAnIngestPlugin() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        assertTrue(plugin instanceof IngestPlugin);
    }

    public void testFeatureProcessorIsAdded() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        Map<String, Processor.Factory> processors = plugin.getProcessors(null);
        assertTrue(processors.containsKey(FeatureProcessor.TYPE));
        assertTrue(processors.get(FeatureProcessor.TYPE) instanceof FeatureProcessor.Factory);
    }

    public void testTotalRestHandlers() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        assertEquals(SUPPORTED_REST_HANDLERS.size(), plugin.getRestHandlers(Settings.EMPTY, null, null, null, null, null, null).size());
    }

    public void testUploadGeoJSONTransportIsAdded() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        final List<ActionPlugin.ActionHandler<? extends ActionRequest, ? extends ActionResponse>> actions = plugin.getActions();
        assertEquals(1, actions.stream().filter(actionHandler -> actionHandler.getAction() instanceof UploadGeoJSONAction).count());
    }
}
