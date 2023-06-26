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

import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.component.LifecycleComponent;
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
import org.opensearch.geospatial.ip2geo.action.DeleteDatasourceAction;
import org.opensearch.geospatial.ip2geo.action.DeleteDatasourceTransportAction;
import org.opensearch.geospatial.ip2geo.action.GetDatasourceAction;
import org.opensearch.geospatial.ip2geo.action.GetDatasourceTransportAction;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceAction;
import org.opensearch.geospatial.ip2geo.action.PutDatasourceTransportAction;
import org.opensearch.geospatial.ip2geo.action.RestDeleteDatasourceHandler;
import org.opensearch.geospatial.ip2geo.action.RestGetDatasourceHandler;
import org.opensearch.geospatial.ip2geo.action.RestPutDatasourceHandler;
import org.opensearch.geospatial.ip2geo.action.RestUpdateDatasourceHandler;
import org.opensearch.geospatial.ip2geo.action.UpdateDatasourceAction;
import org.opensearch.geospatial.ip2geo.action.UpdateDatasourceTransportAction;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutor;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.dao.DatasourceDao;
import org.opensearch.geospatial.ip2geo.dao.GeoIpDataDao;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoCachedDao;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceRunner;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceUpdateService;
import org.opensearch.geospatial.ip2geo.listener.Ip2GeoListener;
import org.opensearch.geospatial.ip2geo.processor.Ip2GeoProcessor;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexGrid;
import org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.opensearch.geospatial.stats.upload.RestUploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.geospatial.stats.upload.UploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStatsTransportAction;
import org.opensearch.index.IndexModule;
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
@Log4j2
public class GeospatialPlugin extends Plugin implements IngestPlugin, ActionPlugin, MapperPlugin, SearchPlugin, SystemIndexPlugin {
    private Ip2GeoCachedDao ip2GeoCachedDao;
    private DatasourceDao datasourceDao;
    private GeoIpDataDao geoIpDataDao;

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(new SystemIndexDescriptor(IP2GEO_DATA_INDEX_NAME_PREFIX, "System index used for Ip2Geo data"));
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        this.datasourceDao = new DatasourceDao(parameters.client, parameters.ingestService.getClusterService());
        this.geoIpDataDao = new GeoIpDataDao(parameters.ingestService.getClusterService(), parameters.client);
        this.ip2GeoCachedDao = new Ip2GeoCachedDao(datasourceDao);
        return MapBuilder.<String, Processor.Factory>newMapBuilder()
            .put(FeatureProcessor.TYPE, new FeatureProcessor.Factory())
            .put(Ip2GeoProcessor.TYPE, new Ip2GeoProcessor.Factory(parameters.ingestService, datasourceDao, geoIpDataDao, ip2GeoCachedDao))
            .immutableMap();
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (DatasourceExtension.JOB_INDEX_NAME.equals(indexModule.getIndex().getName())) {
            indexModule.addIndexOperationListener(ip2GeoCachedDao);
            log.info("Ip2GeoListener started listening to operations on index {}", DatasourceExtension.JOB_INDEX_NAME);
        }
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.of(Ip2GeoListener.class);
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(Ip2GeoExecutor.executorBuilder(settings));
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
        DatasourceUpdateService datasourceUpdateService = new DatasourceUpdateService(clusterService, datasourceDao, geoIpDataDao);
        Ip2GeoExecutor ip2GeoExecutor = new Ip2GeoExecutor(threadPool);
        Ip2GeoLockService ip2GeoLockService = new Ip2GeoLockService(clusterService, client);
        /**
         * We don't need to return datasource runner because it is used only by job scheduler and job scheduler
         * does not use DI but it calls DatasourceExtension#getJobRunner to get DatasourceRunner instance.
         */
        DatasourceRunner.getJobRunnerInstance()
            .initialize(clusterService, datasourceUpdateService, ip2GeoExecutor, datasourceDao, ip2GeoLockService);

        return List.of(
            UploadStats.getInstance(),
            datasourceUpdateService,
            datasourceDao,
            ip2GeoExecutor,
            geoIpDataDao,
            ip2GeoLockService,
            ip2GeoCachedDao
        );
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
        List<RestHandler> geoJsonHandlers = List.of(new RestUploadStatsAction(), new RestUploadGeoJSONAction());

        List<RestHandler> ip2geoHandlers = List.of(
            new RestPutDatasourceHandler(clusterSettings),
            new RestGetDatasourceHandler(),
            new RestUpdateDatasourceHandler(),
            new RestDeleteDatasourceHandler()
        );

        List<RestHandler> allHandlers = new ArrayList<>();
        allHandlers.addAll(geoJsonHandlers);
        allHandlers.addAll(ip2geoHandlers);
        return allHandlers;
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> geoJsonHandlers = List.of(
            new ActionHandler<>(UploadGeoJSONAction.INSTANCE, UploadGeoJSONTransportAction.class),
            new ActionHandler<>(UploadStatsAction.INSTANCE, UploadStatsTransportAction.class)
        );

        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> ip2geoHandlers = List.of(
            new ActionHandler<>(PutDatasourceAction.INSTANCE, PutDatasourceTransportAction.class),
            new ActionHandler<>(GetDatasourceAction.INSTANCE, GetDatasourceTransportAction.class),
            new ActionHandler<>(UpdateDatasourceAction.INSTANCE, UpdateDatasourceTransportAction.class),
            new ActionHandler<>(DeleteDatasourceAction.INSTANCE, DeleteDatasourceTransportAction.class)
        );

        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> allHandlers = new ArrayList<>();
        allHandlers.addAll(geoJsonHandlers);
        allHandlers.addAll(ip2geoHandlers);
        return allHandlers;
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
