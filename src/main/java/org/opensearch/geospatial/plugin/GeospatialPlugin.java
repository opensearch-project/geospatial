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
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.Lifecycle;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.common.lifecycle.LifecycleListener;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.geospatial.action.IpEnrichmentAction;
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
import org.opensearch.geospatial.ip2geo.action.IpEnrichmentTransportAction;
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
import org.opensearch.geospatial.ip2geo.common.URLDenyListChecker;
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
import org.opensearch.geospatial.shared.PluginClient;
import org.opensearch.geospatial.stats.upload.RestUploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.geospatial.stats.upload.UploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStatsTransportAction;
import org.opensearch.identity.PluginSubject;
import org.opensearch.index.IndexModule;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.ingest.Processor;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.IdentityAwarePlugin;
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
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import lombok.extern.log4j.Log4j2;

/**
 * Entry point for Geospatial features. It provides additional Processors, Actions
 * to interact with Cluster.
 */
@Log4j2
public class GeospatialPlugin extends Plugin
    implements
        IngestPlugin,
        ActionPlugin,
        MapperPlugin,
        SearchPlugin,
        SystemIndexPlugin,
        ClusterPlugin,
        IdentityAwarePlugin {
    private Ip2GeoCachedDao ip2GeoCachedDao;
    private DatasourceDao datasourceDao;
    private GeoIpDataDao geoIpDataDao;
    private Ip2GeoProcessor.Factory ip2geoProcessor;
    private URLDenyListChecker urlDenyListChecker;
    private ClusterService clusterService;
    private Ip2GeoLockService ip2GeoLockService;
    private Ip2GeoExecutor ip2GeoExecutor;
    private DatasourceUpdateService datasourceUpdateService;
    private PluginClient pluginClient;

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(IP2GEO_DATA_INDEX_NAME_PREFIX + "*", "System index pattern used for Ip2Geo data"),
            new SystemIndexDescriptor(DatasourceExtension.JOB_INDEX_NAME, "System index used for Ip2Geo job")
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        this.ip2geoProcessor = new Ip2GeoProcessor.Factory(parameters.ingestService);
        return MapBuilder.<String, Processor.Factory>newMapBuilder()
            .put(FeatureProcessor.TYPE, new FeatureProcessor.Factory())
            .put(Ip2GeoProcessor.TYPE, ip2geoProcessor)
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
        final List<Class<? extends LifecycleComponent>> services = new ArrayList<>(2);
        services.add(Ip2GeoListener.class);
        services.add(GuiceHolder.class);
        return services;
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
        this.clusterService = clusterService;
        this.pluginClient = new PluginClient(client);
        this.urlDenyListChecker = new URLDenyListChecker(clusterService.getClusterSettings());
        this.datasourceDao = new DatasourceDao(pluginClient, clusterService);
        this.geoIpDataDao = new GeoIpDataDao(clusterService, pluginClient, urlDenyListChecker);
        this.ip2GeoCachedDao = new Ip2GeoCachedDao(clusterService, datasourceDao, geoIpDataDao);
        if (this.ip2geoProcessor != null) {
            this.ip2geoProcessor.initialize(datasourceDao, geoIpDataDao, ip2GeoCachedDao);
        }
        this.datasourceUpdateService = new DatasourceUpdateService(clusterService, datasourceDao, geoIpDataDao, urlDenyListChecker);
        this.ip2GeoExecutor = new Ip2GeoExecutor(threadPool);
        this.ip2GeoLockService = new Ip2GeoLockService(clusterService);

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
            new RestPutDatasourceHandler(clusterSettings, urlDenyListChecker),
            new RestGetDatasourceHandler(),
            new RestUpdateDatasourceHandler(urlDenyListChecker),
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

        // Inter-cluster IP enrichment request
        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> ipEnrichmentHandlers = List.of(
            new ActionHandler<>(IpEnrichmentAction.INSTANCE, IpEnrichmentTransportAction.class)
        );

        List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> allHandlers = new ArrayList<>();
        allHandlers.addAll(geoJsonHandlers);
        allHandlers.addAll(ip2geoHandlers);
        allHandlers.addAll(ipEnrichmentHandlers);
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

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        LockService lockService = GuiceHolder.getLockService();
        ip2GeoLockService.initialize(lockService);

        DatasourceRunner.getJobRunnerInstance()
            .initialize(this.clusterService, this.datasourceUpdateService, this.ip2GeoExecutor, this.datasourceDao, this.ip2GeoLockService);
    }

    @Override
    public void assignSubject(PluginSubject pluginSubject) {
        // When security is not installed, the pluginSubject will still be assigned.
        assert pluginSubject != null;
        this.pluginClient.setSubject(pluginSubject);
    }

    public static class GuiceHolder implements LifecycleComponent {

        private static LockService lockService;

        @Inject
        public GuiceHolder(final LockService lockService) {
            GuiceHolder.lockService = lockService;
        }

        static LockService getLockService() {
            return lockService;
        }

        @Override
        public void close() {}

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {}

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {}

        @Override
        public void start() {}

        @Override
        public void stop() {}

    }
}
