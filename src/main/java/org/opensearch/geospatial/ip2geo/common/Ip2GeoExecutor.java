/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import java.util.concurrent.ExecutorService;

import org.opensearch.common.settings.Settings;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;
import org.opensearch.threadpool.ThreadPool;

/**
 * Provide a list of static methods related with executors for Ip2Geo
 */
public class Ip2GeoExecutor {
    private static final String THREAD_POOL_NAME = "_plugin_geospatial_ip2geo_datasource_update";
    private final ThreadPool threadPool;

    public Ip2GeoExecutor(final ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    /**
     * We use fixed thread count of 1 for updating datasource as updating datasource is running background
     * once a day at most and no need to expedite the task.
     *
     * @param settings the settings
     * @return the executor builder
     */
    public static ExecutorBuilder executorBuilder(final Settings settings) {
        return new FixedExecutorBuilder(settings, THREAD_POOL_NAME, 1, 1000, THREAD_POOL_NAME, false);
    }

    /**
     * Return an executor service for datasource update task
     *
     * @return the executor service
     */
    public ExecutorService forDatasourceUpdate() {
        return threadPool.executor(THREAD_POOL_NAME);
    }
}
