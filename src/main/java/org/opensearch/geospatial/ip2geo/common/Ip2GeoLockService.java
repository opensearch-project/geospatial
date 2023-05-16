/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.opensearch.geospatial.ip2geo.jobscheduler.DatasourceExtension.JOB_INDEX_NAME;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.log4j.Log4j2;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.utils.LockService;

/**
 * A wrapper of job scheduler's lock service for datasource
 */
@Log4j2
public class Ip2GeoLockService {
    public static final long LOCK_DURATION_IN_SECONDS = 300l;
    public static final long RENEW_AFTER_IN_SECONDS = 120l;
    private final ClusterService clusterService;
    private final LockService lockService;

    /**
     * Constructor
     *
     * @param clusterService the cluster service
     * @param client the client
     */
    public Ip2GeoLockService(final ClusterService clusterService, final Client client) {
        this.clusterService = clusterService;
        this.lockService = new LockService(client, clusterService);
    }

    /**
     * Wrapper method of LockService#acquireLockWithId
     *
     * Datasource use its name as doc id in job scheduler. Therefore, we can use datasource name to acquire
     * a lock on a datasource.
     *
     * @param datasourceName datasourceName to acquire lock on
     * @param lockDurationSeconds the lock duration in seconds
     * @param listener the listener
     */
    public void acquireLock(final String datasourceName, final Long lockDurationSeconds, final ActionListener<LockModel> listener) {
        lockService.acquireLockWithId(JOB_INDEX_NAME, lockDurationSeconds, datasourceName, listener);
    }

    /**
     * Wrapper method of LockService#release
     *
     * @param lockModel the lock model
     */
    public void releaseLock(final LockModel lockModel) {
        lockService.release(
            lockModel,
            ActionListener.wrap(released -> {}, exception -> log.error("Failed to release the lock", exception))
        );
    }

    /**
     * Synchronous method of LockService#renewLock
     *
     * @param lockModel lock to renew
     * @return renewed lock if renew succeed and null otherwise
     */
    public LockModel renewLock(final LockModel lockModel) {
        AtomicReference<LockModel> lockReference = new AtomicReference();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        lockService.renewLock(lockModel, new ActionListener<>() {
            @Override
            public void onResponse(final LockModel lockModel) {
                lockReference.set(lockModel);
                countDownLatch.countDown();
            }

            @Override
            public void onFailure(final Exception e) {
                lockReference.set(null);
                countDownLatch.countDown();
            }
        });

        try {
            countDownLatch.await(clusterService.getClusterSettings().get(Ip2GeoSettings.TIMEOUT).getSeconds(), TimeUnit.SECONDS);
            return lockReference.get();
        } catch (InterruptedException e) {
            return null;
        }
    }

    /**
     * Return a runnable which can renew the given lock model
     *
     * The runnable renews the lock and store the renewed lock in the AtomicReference.
     * It only renews the lock when it passed {@code RENEW_AFTER_IN_SECONDS} since
     * the last time the lock was renewed to avoid resource abuse.
     *
     * @param lockModel lock model to renew
     * @return runnable which can renew the given lock for every call
     */
    public Runnable getRenewLockRunnable(final AtomicReference<LockModel> lockModel) {
        return () -> {
            LockModel preLock = lockModel.get();
            if (Instant.now().isBefore(preLock.getLockTime().plusSeconds(RENEW_AFTER_IN_SECONDS))) {
                return;
            }
            lockModel.set(renewLock(lockModel.get()));
            if (lockModel.get() == null) {
                new OpenSearchException("failed to renew a lock [{}]", preLock);
            }
        };
    }
}
