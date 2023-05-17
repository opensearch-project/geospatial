/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

/**
 * Provide a list of static methods related with executors for Ip2Geo
 */
public class Ip2GeoExecutor {
    private static final String THREAD_POOL_NAME_DATASOURCE_CREATE = "_plugin_geospatial_ip2geo_datasource_create";
    private static final String THREAD_POOL_NAME_DATASOURCE_UPDATE = "_plugin_geospatial_ip2geo_datasource_update";
    private final ThreadPool threadPool;

    public Ip2GeoExecutor(final ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * Datasource creation thread pool:
     * Use fixed thread count of 1 with zero queue size for creating datasource.
     * This is to reduce the load on cluster by preventing multiple creation requests to be processed concurrently in a node.
     * Queue size is zero as task in queue cannot renew lock which will cause the datasource to be modified by another request meanwhile.
     *
     * Datasource update thread pool:
     * Use fixed thread count of 1 for updating datasource as updating datasource is running background
     * once a day at most and no need to expedite the task.
     *
     * @param settings the settings
     * @return the executor builder
     */
    public static List<ExecutorBuilder<?>> executorBuilder(final Settings settings) {
        List<ExecutorBuilder<?>> executorBuilders = new ArrayList<>();
        executorBuilders.add(
            new FixedExecutorBuilder(settings, THREAD_POOL_NAME_DATASOURCE_CREATE, 1, 0, THREAD_POOL_NAME_DATASOURCE_CREATE, false)
        );
        executorBuilders.add(
            new FixedExecutorBuilder(settings, THREAD_POOL_NAME_DATASOURCE_UPDATE, 1, 1000, THREAD_POOL_NAME_DATASOURCE_UPDATE, false)
        );
        return executorBuilders;
    }

    /**
     * Return an executor service for datasource update task
     *
     * @return the executor service
     */
    public ExecutorService forDatasourceUpdate() {
        return threadPool.executor(THREAD_POOL_NAME_DATASOURCE_UPDATE);
    }

    /**
     * Return an executor service for datasource create task
     *
     * @return the executor service
     */
    public ExecutorService forDatasourceCreate() {
        return threadPool.executor(THREAD_POOL_NAME_DATASOURCE_CREATE);
    }
}
