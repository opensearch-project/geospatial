/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.plugin;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.geospatial.action.upload.geojson.UploadGeoJSONAction;
import org.opensearch.geospatial.ip2geo.action.RestGetDatasourceHandler;
import org.opensearch.geospatial.ip2geo.action.RestPutDatasourceHandler;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataFacade;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutor;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceUpdateService;
import org.opensearch.geospatial.processor.FeatureProcessor;
import org.opensearch.geospatial.rest.action.upload.geojson.RestUploadGeoJSONAction;
import org.opensearch.geospatial.stats.upload.RestUploadStatsAction;
import org.opensearch.geospatial.stats.upload.UploadStats;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.ingest.IngestService;
import org.opensearch.ingest.Processor;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

public class GeospatialPluginTests extends OpenSearchTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, new HashSet(Ip2GeoSettings.settings()));
    private final List<RestHandler> SUPPORTED_REST_HANDLERS = List.of(
        new RestUploadGeoJSONAction(),
        new RestUploadStatsAction(),
        new RestPutDatasourceHandler(clusterSettings),
        new RestGetDatasourceHandler()
    );

    private final Set<String> SUPPORTED_SYSTEM_INDEX_PATTERN = Set.of(IP2GEO_DATA_INDEX_NAME_PREFIX);

    private final Set<Class> SUPPORTED_COMPONENTS = Set.of(
        UploadStats.class,
        DatasourceUpdateService.class,
        DatasourceFacade.class,
        Ip2GeoExecutor.class,
        GeoIpDataFacade.class,
        Ip2GeoLockService.class
    );

    @Mock
    private Client client;
    @Mock
    private ClusterService clusterService;
    @Mock
    private IngestService ingestService;
    @Mock
    private ThreadPool threadPool;
    @Mock
    private ResourceWatcherService resourceWatcherService;
    @Mock
    private ScriptService scriptService;
    @Mock
    private NamedXContentRegistry xContentRegistry;
    @Mock
    private Environment environment;
    @Mock
    private NamedWriteableRegistry namedWriteableRegistry;
    @Mock
    private IndexNameExpressionResolver indexNameExpressionResolver;
    @Mock
    private Supplier<RepositoriesService> repositoriesServiceSupplier;
    private NodeEnvironment nodeEnvironment;
    private Settings settings;
    private AutoCloseable openMocks;

    @Before
    public void init() {
        openMocks = MockitoAnnotations.openMocks(this);
        settings = Settings.EMPTY;
        when(client.settings()).thenReturn(settings);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);
        when(ingestService.getClusterService()).thenReturn(clusterService);
        nodeEnvironment = null;
    }

    @After
    public void close() throws Exception {
        openMocks.close();
    }

    public void testSystemIndexDescriptors() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        Set<String> registeredSystemIndexPatterns = new HashSet<>();
        for (SystemIndexDescriptor descriptor : plugin.getSystemIndexDescriptors(Settings.EMPTY)) {
            registeredSystemIndexPatterns.add(descriptor.getIndexPattern());
        }
        assertEquals(SUPPORTED_SYSTEM_INDEX_PATTERN, registeredSystemIndexPatterns);

    }

    public void testExecutorBuilders() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        assertEquals(1, plugin.getExecutorBuilders(Settings.EMPTY).size());
    }

    public void testCreateComponents() {
        GeospatialPlugin plugin = new GeospatialPlugin();
        Set<Class> registeredComponents = new HashSet<>();
        Collection<Object> components = plugin.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier
        );
        for (Object component : components) {
            registeredComponents.add(component.getClass());
        }
        assertEquals(SUPPORTED_COMPONENTS, registeredComponents);
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
