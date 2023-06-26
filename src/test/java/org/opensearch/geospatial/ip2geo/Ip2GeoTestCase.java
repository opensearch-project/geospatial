/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo;

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Randomness;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.common.DatasourceState;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoExecutor;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService;
import org.opensearch.geospatial.ip2geo.common.Ip2GeoSettings;
import org.opensearch.geospatial.ip2geo.dao.DatasourceDao;
import org.opensearch.geospatial.ip2geo.dao.GeoIpDataDao;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoCachedDao;
import org.opensearch.geospatial.ip2geo.dao.Ip2GeoProcessorDao;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceTask;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceUpdateService;
import org.opensearch.geospatial.ip2geo.processor.Ip2GeoProcessor;
import org.opensearch.ingest.IngestMetadata;
import org.opensearch.ingest.IngestService;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskListener;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.RestActionTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

public abstract class Ip2GeoTestCase extends RestActionTestCase {
    @Mock
    protected ClusterService clusterService;
    @Mock
    protected DatasourceUpdateService datasourceUpdateService;
    @Mock
    protected DatasourceDao datasourceDao;
    @Mock
    protected Ip2GeoExecutor ip2GeoExecutor;
    @Mock
    protected GeoIpDataDao geoIpDataDao;
    @Mock
    protected Ip2GeoCachedDao ip2GeoCachedDao;
    @Mock
    protected ClusterState clusterState;
    @Mock
    protected Metadata metadata;
    @Mock
    protected IngestService ingestService;
    @Mock
    protected ActionFilters actionFilters;
    @Mock
    protected ThreadPool threadPool;
    @Mock
    protected TransportService transportService;
    @Mock
    protected Ip2GeoLockService ip2GeoLockService;
    @Mock
    protected Ip2GeoProcessorDao ip2GeoProcessorDao;
    protected IngestMetadata ingestMetadata;
    protected NoOpNodeClient client;
    protected VerifyingClient verifyingClient;
    protected LockService lockService;
    protected ClusterSettings clusterSettings;
    protected Settings settings;
    private AutoCloseable openMocks;

    @Before
    public void prepareIp2GeoTestCase() {
        openMocks = MockitoAnnotations.openMocks(this);
        settings = Settings.EMPTY;
        client = new NoOpNodeClient(this.getTestName());
        verifyingClient = spy(new VerifyingClient(this.getTestName()));
        clusterSettings = new ClusterSettings(settings, new HashSet<>(Ip2GeoSettings.settings()));
        lockService = new LockService(client, clusterService);
        ingestMetadata = new IngestMetadata(Collections.emptyMap());
        when(metadata.custom(IngestMetadata.TYPE)).thenReturn(ingestMetadata);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(clusterState.getMetadata()).thenReturn(metadata);
        when(clusterState.routingTable()).thenReturn(RoutingTable.EMPTY_ROUTING_TABLE);
        when(ip2GeoExecutor.forDatasourceUpdate()).thenReturn(OpenSearchExecutors.newDirectExecutorService());
        when(ingestService.getClusterService()).thenReturn(clusterService);
        when(threadPool.generic()).thenReturn(OpenSearchExecutors.newDirectExecutorService());
    }

    @After
    public void clean() throws Exception {
        openMocks.close();
        client.close();
        verifyingClient.close();
    }

    protected DatasourceState randomStateExcept(DatasourceState state) {
        assertNotNull(state);
        return Arrays.stream(DatasourceState.values())
            .sequential()
            .filter(s -> !s.equals(state))
            .collect(Collectors.toList())
            .get(Randomness.createSecure().nextInt(DatasourceState.values().length - 2));
    }

    protected DatasourceState randomState() {
        return Arrays.stream(DatasourceState.values())
            .sequential()
            .collect(Collectors.toList())
            .get(Randomness.createSecure().nextInt(DatasourceState.values().length - 1));
    }

    protected DatasourceTask randomTask() {
        return Arrays.stream(DatasourceTask.values())
            .sequential()
            .collect(Collectors.toList())
            .get(Randomness.createSecure().nextInt(DatasourceTask.values().length - 1));
    }

    protected String randomIpAddress() {
        return String.format(
            Locale.ROOT,
            "%d.%d.%d.%d",
            Randomness.get().nextInt(255),
            Randomness.get().nextInt(255),
            Randomness.get().nextInt(255),
            Randomness.get().nextInt(255)
        );
    }

    @SneakyThrows
    @SuppressForbidden(reason = "unit test")
    protected String sampleManifestUrl() {
        return Paths.get(this.getClass().getClassLoader().getResource("ip2geo/manifest.json").toURI()).toUri().toURL().toExternalForm();
    }

    @SuppressForbidden(reason = "unit test")
    protected String sampleManifestUrlWithInvalidUrl() throws Exception {
        return Paths.get(this.getClass().getClassLoader().getResource("ip2geo/manifest_invalid_url.json").toURI())
            .toUri()
            .toURL()
            .toExternalForm();
    }

    @SuppressForbidden(reason = "unit test")
    protected File sampleIp2GeoFile() {
        return new File(this.getClass().getClassLoader().getResource("ip2geo/sample_valid.csv").getFile());
    }

    protected long randomPositiveLong() {
        long value = Randomness.get().nextLong();
        return value < 0 ? -value : value;
    }

    /**
     * Update interval should be > 0 and < validForInDays.
     * For an update test to work, there should be at least one eligible value other than current update interval.
     * Therefore, the smallest value for validForInDays is 2.
     * Update interval is random value from 1 to validForInDays - 2.
     * The new update value will be validForInDays - 1.
     */
    protected Datasource randomDatasource(final Instant updateStartTime) {
        int validForInDays = 3 + Randomness.get().nextInt(30);
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Datasource datasource = new Datasource();
        datasource.setName(GeospatialTestHelper.randomLowerCaseString());
        datasource.setUserSchedule(
            new IntervalSchedule(
                updateStartTime.truncatedTo(ChronoUnit.MILLIS),
                1 + Randomness.get().nextInt(validForInDays - 2),
                ChronoUnit.DAYS
            )
        );
        datasource.setSystemSchedule(datasource.getUserSchedule());
        datasource.setTask(randomTask());
        datasource.setState(randomState());
        datasource.setCurrentIndex(datasource.newIndexName(UUID.randomUUID().toString()));
        datasource.setIndices(Arrays.asList(GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString()));
        datasource.setEndpoint(String.format(Locale.ROOT, "https://%s.com/manifest.json", GeospatialTestHelper.randomLowerCaseString()));
        datasource.getDatabase()
            .setFields(Arrays.asList(GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString()));
        datasource.getDatabase().setProvider(GeospatialTestHelper.randomLowerCaseString());
        datasource.getDatabase().setUpdatedAt(now);
        datasource.getDatabase().setSha256Hash(GeospatialTestHelper.randomLowerCaseString());
        datasource.getDatabase().setValidForInDays((long) validForInDays);
        datasource.getUpdateStats().setLastSkippedAt(now);
        datasource.getUpdateStats().setLastSucceededAt(now);
        datasource.getUpdateStats().setLastFailedAt(now);
        datasource.getUpdateStats().setLastProcessingTimeInMillis(randomPositiveLong());
        datasource.setLastUpdateTime(now);
        if (Randomness.get().nextInt() % 2 == 0) {
            datasource.enable();
        } else {
            datasource.disable();
        }
        return datasource;
    }

    protected Datasource randomDatasource() {
        return randomDatasource(Instant.now());
    }

    protected LockModel randomLockModel() {
        LockModel lockModel = new LockModel(
            GeospatialTestHelper.randomLowerCaseString(),
            GeospatialTestHelper.randomLowerCaseString(),
            Instant.now(),
            randomPositiveLong(),
            false
        );
        return lockModel;
    }

    protected Ip2GeoProcessor randomIp2GeoProcessor(String datasourceName) {
        String tag = GeospatialTestHelper.randomLowerCaseString();
        String description = GeospatialTestHelper.randomLowerCaseString();
        String field = GeospatialTestHelper.randomLowerCaseString();
        String targetField = GeospatialTestHelper.randomLowerCaseString();
        Set<String> properties = Set.of(GeospatialTestHelper.randomLowerCaseString());
        Ip2GeoProcessor ip2GeoProcessor = new Ip2GeoProcessor(
            tag,
            description,
            field,
            targetField,
            datasourceName,
            properties,
            true,
            clusterSettings,
            datasourceDao,
            geoIpDataDao,
            ip2GeoCachedDao
        );
        return ip2GeoProcessor;
    }

    /**
     * Temporary class of VerifyingClient until this PR(https://github.com/opensearch-project/OpenSearch/pull/7167)
     * is merged in OpenSearch core
     */
    public static class VerifyingClient extends NoOpNodeClient {
        AtomicReference<BiFunction> executeVerifier = new AtomicReference<>();
        AtomicReference<BiFunction> executeLocallyVerifier = new AtomicReference<>();

        public VerifyingClient(String testName) {
            super(testName);
            reset();
        }

        /**
         * Clears any previously set verifier functions set by {@link #setExecuteVerifier(BiFunction)} and/or
         * {@link #setExecuteLocallyVerifier(BiFunction)}. These functions are replaced with functions which will throw an
         * {@link AssertionError} if called.
         */
        public void reset() {
            executeVerifier.set((arg1, arg2) -> { throw new AssertionError(); });
            executeLocallyVerifier.set((arg1, arg2) -> { throw new AssertionError(); });
        }

        /**
         * Sets the function that will be called when {@link #doExecute(ActionType, ActionRequest, ActionListener)} is called. The given
         * function should return either a subclass of {@link ActionResponse} or {@code null}.
         * @param verifier A function which is called in place of {@link #doExecute(ActionType, ActionRequest, ActionListener)}
         */
        public <Request extends ActionRequest, Response extends ActionResponse> void setExecuteVerifier(
            BiFunction<ActionType<Response>, Request, Response> verifier
        ) {
            executeVerifier.set(verifier);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            try {
                listener.onResponse((Response) executeVerifier.get().apply(action, request));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

        /**
         * Sets the function that will be called when {@link #executeLocally(ActionType, ActionRequest, TaskListener)}is called. The given
         * function should return either a subclass of {@link ActionResponse} or {@code null}.
         * @param verifier A function which is called in place of {@link #executeLocally(ActionType, ActionRequest, TaskListener)}
         */
        public <Request extends ActionRequest, Response extends ActionResponse> void setExecuteLocallyVerifier(
            BiFunction<ActionType<Response>, Request, Response> verifier
        ) {
            executeLocallyVerifier.set(verifier);
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
            ActionType<Response> action,
            Request request,
            ActionListener<Response> listener
        ) {
            listener.onResponse((Response) executeLocallyVerifier.get().apply(action, request));
            return null;
        }

        @Override
        public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
            ActionType<Response> action,
            Request request,
            TaskListener<Response> listener
        ) {
            listener.onResponse(null, (Response) executeLocallyVerifier.get().apply(action, request));
            return null;
        }

    }
}
