/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONAction;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONTransportAction;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldMapper;
import org.opensearch.geospatial.index.mapper.xypoint.XYPointFieldTypeParser;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldMapper;
import org.opensearch.geospatial.index.mapper.xyshape.XYShapeFieldTypeParser;
import org.opensearch.geospatial.index.query.xyshape.XYShapeQueryBuilder;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceAction;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceTransportAction;
import org.opensearch.geospatial.ip2geo.action.RestPutDatasourceHandler;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutorHelper;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceRunner;
import org.opensearch.geospatial.ip2geo.processor.Ip2GeoCache;
import org.opensearch.geospatial.ip2geo.processor.Ip2GeoProcessor;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexGrid;
import org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.opensearch.geospatial.stats.upload.RestUploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.geospatial.stats.upload.UploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStatsTransportAction;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * Entry point for Geospatial features. It provides additional Processors, Actions
 * to interact with Cluster.
 */
public class GeospatialPlugin extends Plugin implements IngestPlugin, ActionPlugin, MapperPlugin, SearchPlugin, SystemIndexPlugin {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(new SystemIndexDescriptor(IP2GEO_DATA_INDEX_NAME_PREFIX, "System index used for Ip2Geo data"));
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        return MapBuilder.<String, Processor.Factory>newMapBuilder()
            .put(FeatureProcessor.TYPE, new FeatureProcessor.Factory())
            .put(
                Ip2GeoProcessor.TYPE,
                new Ip2GeoProcessor.Factory(
                    new Ip2GeoCache(Ip2GeoSettings.CACHE_SIZE.get(parameters.client.settings())),
                    parameters.client,
                    parameters.ingestService
                )
            )
            .immutableMap();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(Ip2GeoExecutorHelper.executorBuilder(settings));
        return executorBuilders;
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Ip2GeoSettings.settings();
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        // Initialize DatasourceUpdateRunner
        DatasourceRunner.getJobRunnerInstance().initialize(clusterService, threadPool, client);

        return List.of(UploadStats.getInstance());
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
        return List.of(new RestUploadStatsAction(), new RestUploadGeoJSONAction(), new RestPutDatasourceHandler(settings, clusterSettings));
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(UploadGeoJSONAction.INSTANCE, UploadGeoJSONTransportAction.class),
            new ActionHandler<>(UploadStatsAction.INSTANCE, UploadStatsTransportAction.class),
            new ActionHandler<>(PutDatasourceAction.INSTANCE, PutDatasourceTransportAction.class)
        );
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Map.of(
            XYShapeFieldMapper.CONTENT_TYPE,
            new XYShapeFieldTypeParser(),
            XYPointFieldMapper.CONTENT_TYPE,
            new XYPointFieldTypeParser()
        );
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        // Register XYShapeQuery Builder to be delegated for query type: xy_shape
        return List.of(new QuerySpec<>(XYShapeQueryBuilder.NAME, XYShapeQueryBuilder::new, XYShapeQueryBuilder::fromXContent));
    }

    /**
     * Registering {@link GeoHexGrid} aggregation on GeoPoint field.
     */
    @Override
    public List<AggregationSpec> getAggregations() {

        final var geoHexGridSpec = new AggregationSpec(
            GeoHexGridAggregationBuilder.NAME,
            GeoHexGridAggregationBuilder::new,
            GeoHexGridAggregationBuilder.PARSER
        ).addResultReader(GeoHexGrid::new).setAggregatorRegistrar(GeoHexGridAggregationBuilder::registerAggregators);

        return List.of(geoHexGridSpec);
    }
}
