/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
public class Ip2GeoExecutorHelper {
    private static final String THREAD_POOL_NAME = "_plugin_geospatial_ip2geo_datasource_update";

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
     * @param threadPool the thread pool
     * @return the executor service
     */
    public static ExecutorService forDatasourceUpdate(final ThreadPool threadPool) {
        return threadPool.executor(THREAD_POOL_NAME);
    }
}
