/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import static java.util.Collections.singletonList;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONAction;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONTransportAction;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;

/**
 * Entry point for Geospatial features. It provides additional Processors, Actions
 * to interact with Cluster.
 */
public class GeospatialPlugin extends Plugin implements IngestPlugin, ActionPlugin {

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return MapBuilder.<String, Processor.Factory>newMapBuilder()
            .put(FeatureProcessor.TYPE, new FeatureProcessor.Factory())
            .immutableMap();
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        RestUploadGeoJSONAction uploadGeoJSONAction = new RestUploadGeoJSONAction();
        return singletonList(uploadGeoJSONAction);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return singletonList(new ActionHandler<>(UploadGeoJSONAction.INSTANCE, UploadGeoJSONTransportAction.class));
    }
}
