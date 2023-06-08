/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.listener;

import static org.opensearch.geospatial.ip2geo.jobscheduler.Datasource.IP2GEO_DATA_INDEX_NAME_PREFIX;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.RestoreInProgress;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.component.AbstractLifecycleComponent;
import org.opensearch.common.inject.Inject;
import org.opensearch.geospatial.ip2geo.common.DatasourceFacade;
import org.opensearch.geospatial.ip2geo.common.GeoIpDataFacade;
import org.opensearch.geospatial.ip2geo.jobscheduler.Datasource;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension;
import org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceTask;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.threadpool.ThreadPool;

@Log4j2
@AllArgsConstructor(onConstructor = @__(@Inject))
public class Ip2GeoListener extends AbstractLifecycleComponent implements ClusterStateListener {
    private static final int SCHEDULE_IN_MIN = 15;
    private static final int DELAY_IN_MILLIS = 10000;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final DatasourceFacade datasourceFacade;
    private final GeoIpDataFacade geoIpDataFacade;

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.localNodeClusterManager() == false) {
            return;
        }

        for (RestoreInProgress.Entry entry : event.state().custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY)) {
            if (RestoreInProgress.State.SUCCESS.equals(entry.state()) == false) {
                continue;
            }

            if (entry.indices().stream().anyMatch(index -> DatasourceExtension.JOB_INDEX_NAME.equals(index))) {
                threadPool.generic().submit(() -> forceUpdateGeoIpData());
            }

            List<String> ip2GeoDataIndices = entry.indices()
                .stream()
                .filter(index -> index.startsWith(IP2GEO_DATA_INDEX_NAME_PREFIX))
                .collect(Collectors.toList());
            if (ip2GeoDataIndices.isEmpty() == false) {
                threadPool.generic().submit(() -> geoIpDataFacade.deleteIp2GeoDataIndex(ip2GeoDataIndices));
            }
        }
    }

    private void forceUpdateGeoIpData() {
        datasourceFacade.getAllDatasources(new ActionListener<>() {
            @Override
            public void onResponse(final List<Datasource> datasources) {
                datasources.stream().forEach(Ip2GeoListener.this::scheduleForceUpdate);
                datasourceFacade.updateDatasource(datasources, new ActionListener<>() {
                    @Override
                    public void onResponse(final BulkResponse bulkItemResponses) {
                        log.info("Datasources are updated for cleanup");
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        log.error("Failed to update datasource for cleanup after restoring", e);
                    }
                });
            }

            @Override
            public void onFailure(final Exception e) {
                log.error("Failed to get datasource after restoring", e);
            }
        });
    }

    /**
     *  Give a delay so that job scheduler can schedule the job right after the delay. Otherwise, it schedules
     *  the job after specified update interval.
     */
    private void scheduleForceUpdate(Datasource datasource) {
        IntervalSchedule schedule = new IntervalSchedule(Instant.now(), SCHEDULE_IN_MIN, ChronoUnit.MINUTES, DELAY_IN_MILLIS);
        datasource.resetDatabase();
        datasource.setSystemSchedule(schedule);
        datasource.setTask(DatasourceTask.ALL);
    }

    @Override
    protected void doStart() {
        if (DiscoveryNode.isClusterManagerNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    protected void doStop() {
        clusterService.removeListener(this);
    }

    @Override
    protected void doClose() throws IOException {

    }
}
