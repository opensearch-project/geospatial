/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.listener;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.common.settings.Settings;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceTask;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;

public class Ip2GeoListenerTests extends Ip2GeoTestCase {
    private Ip2GeoListener ip2GeoListener;

    @Before
    public void init() {
        ip2GeoListener = new Ip2GeoListener(clusterService, threadPool, datasourceDao, geoIpDataDao);
    }

    public void testDoStart_whenClusterManagerNode_thenAddListener() {
        Settings settings = Settings.builder().put("node.roles", "cluster_manager").build();
        when(clusterService.getSettings()).thenReturn(settings);

        // Run
        ip2GeoListener.doStart();

        // Verify
        verify(clusterService).addListener(ip2GeoListener);
    }

    public void testDoStart_whenNotClusterManagerNode_thenDoNotAddListener() {
        Settings settings = Settings.builder().put("node.roles", "data").build();
        when(clusterService.getSettings()).thenReturn(settings);

        // Run
        ip2GeoListener.doStart();

        // Verify
        verify(clusterService, never()).addListener(ip2GeoListener);
    }

    public void testDoStop_whenCalled_thenRemoveListener() {
        // Run
        ip2GeoListener.doStop();

        // Verify
        verify(clusterService).removeListener(ip2GeoListener);
    }

    public void testClusterChanged_whenNotClusterManagerNode_thenDoNothing() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(false);

        // Run
        ip2GeoListener.clusterChanged(event);

        // Verify
        verify(threadPool, never()).generic();
    }

    public void testClusterChanged_whenNotComplete_thenDoNothing() {
        SnapshotId snapshotId = new SnapshotId(GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString());
        Snapshot snapshot = new Snapshot(GeospatialTestHelper.randomLowerCaseString(), snapshotId);
        RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            GeospatialTestHelper.randomLowerCaseString(),
            snapshot,
            RestoreInProgress.State.STARTED,
            Arrays.asList(DatasourceExtension.JOB_INDEX_NAME),
            null
        );
        RestoreInProgress restoreInProgress = new RestoreInProgress.Builder().add(entry).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).thenReturn(restoreInProgress);
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);

        // Run
        ip2GeoListener.clusterChanged(event);

        // Verify
        verify(threadPool, never()).generic();
    }

    public void testClusterChanged_whenNotDatasourceIndex_thenDoNothing() {
        SnapshotId snapshotId = new SnapshotId(GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString());
        Snapshot snapshot = new Snapshot(GeospatialTestHelper.randomLowerCaseString(), snapshotId);
        RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            GeospatialTestHelper.randomLowerCaseString(),
            snapshot,
            RestoreInProgress.State.FAILURE,
            Arrays.asList(GeospatialTestHelper.randomLowerCaseString()),
            null
        );
        RestoreInProgress restoreInProgress = new RestoreInProgress.Builder().add(entry).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).thenReturn(restoreInProgress);
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);

        // Run
        ip2GeoListener.clusterChanged(event);

        // Verify
        verify(threadPool, never()).generic();
    }

    public void testClusterChanged_whenDatasourceIndexIsRestored_thenUpdate() {
        SnapshotId snapshotId = new SnapshotId(GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString());
        Snapshot snapshot = new Snapshot(GeospatialTestHelper.randomLowerCaseString(), snapshotId);
        RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            GeospatialTestHelper.randomLowerCaseString(),
            snapshot,
            RestoreInProgress.State.SUCCESS,
            Arrays.asList(DatasourceExtension.JOB_INDEX_NAME),
            null
        );
        RestoreInProgress restoreInProgress = new RestoreInProgress.Builder().add(entry).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).thenReturn(restoreInProgress);
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);

        // Run
        ip2GeoListener.clusterChanged(event);

        // Verify
        verify(threadPool).generic();
        ArgumentCaptor<ActionListener<List<Datasource>>> captor = ArgumentCaptor.forClass(ActionListener.class);
        verify(datasourceDao).getAllDatasources(captor.capture());

        // Run
        List<Datasource> datasources = Arrays.asList(randomDatasource(), randomDatasource());
        datasources.stream().forEach(datasource -> { datasource.setTask(DatasourceTask.DELETE_UNUSED_INDICES); });

        captor.getValue().onResponse(datasources);

        // Verify
        datasources.stream().forEach(datasource -> {
            assertEquals(DatasourceTask.ALL, datasource.getTask());
            assertNull(datasource.getDatabase().getUpdatedAt());
            assertNull(datasource.getDatabase().getSha256Hash());
            assertTrue(datasource.getSystemSchedule().getNextExecutionTime(Instant.now()).isAfter(Instant.now()));
            assertTrue(datasource.getSystemSchedule().getNextExecutionTime(Instant.now()).isBefore(Instant.now().plusSeconds(60)));
        });
        verify(datasourceDao).updateDatasource(eq(datasources), any());
    }

    public void testClusterChanged_whenGeoIpDataIsRestored_thenDelete() {
        Datasource datasource = randomDatasource();
        SnapshotId snapshotId = new SnapshotId(GeospatialTestHelper.randomLowerCaseString(), GeospatialTestHelper.randomLowerCaseString());
        Snapshot snapshot = new Snapshot(GeospatialTestHelper.randomLowerCaseString(), snapshotId);
        RestoreInProgress.Entry entry = new RestoreInProgress.Entry(
            GeospatialTestHelper.randomLowerCaseString(),
            snapshot,
            RestoreInProgress.State.SUCCESS,
            Arrays.asList(datasource.currentIndexName()),
            null
        );
        RestoreInProgress restoreInProgress = new RestoreInProgress.Builder().add(entry).build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)).thenReturn(restoreInProgress);
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        when(event.localNodeClusterManager()).thenReturn(true);
        when(event.state()).thenReturn(clusterState);

        // Run
        ip2GeoListener.clusterChanged(event);

        // Verify
        verify(threadPool).generic();
        verify(geoIpDataDao).deleteIp2GeoDataIndex(Arrays.asList(datasource.currentIndexName()));
    }

}
