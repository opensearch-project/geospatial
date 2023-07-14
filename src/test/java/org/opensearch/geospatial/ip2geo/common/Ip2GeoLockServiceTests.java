/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.Mockito.mock;
import static org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService.LOCK_DURATION_IN_SECONDS;
import static org.opensearch.geospatial.ip2geo.common.Ip2GeoLockService.RENEW_AFTER_IN_SECONDS;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;
import org.opensearch.jobscheduler.spi.LockModel;

public class Ip2GeoLockServiceTests extends Ip2GeoTestCase {
    private Ip2GeoLockService ip2GeoLockService;
    private Ip2GeoLockService noOpsLockService;

    @Before
    public void init() {
        ip2GeoLockService = new Ip2GeoLockService(clusterService, verifyingClient);
        noOpsLockService = new Ip2GeoLockService(clusterService, client);
    }

    public void testAcquireLock_whenValidInput_thenSucceed() {
        // Cannot test because LockService is final class
        // Simply calling method to increase coverage
        noOpsLockService.acquireLock(GeospatialTestHelper.randomLowerCaseString(), randomPositiveLong(), mock(ActionListener.class));
    }

    public void testAcquireLock_whenCalled_thenNotBlocked() {
        long expectedDurationInMillis = 1000;
        Instant before = Instant.now();
        assertTrue(ip2GeoLockService.acquireLock(null, null).isEmpty());
        Instant after = Instant.now();
        assertTrue(after.toEpochMilli() - before.toEpochMilli() < expectedDurationInMillis);
    }

    public void testReleaseLock_whenValidInput_thenSucceed() {
        // Cannot test because LockService is final class
        // Simply calling method to increase coverage
        noOpsLockService.releaseLock(null);
    }

    public void testRenewLock_whenCalled_thenNotBlocked() {
        long expectedDurationInMillis = 1000;
        Instant before = Instant.now();
        assertNull(ip2GeoLockService.renewLock(null));
        Instant after = Instant.now();
        assertTrue(after.toEpochMilli() - before.toEpochMilli() < expectedDurationInMillis);
    }

    public void testGetRenewLockRunnable_whenLockIsFresh_thenDoNotRenew() {
        LockModel lockModel = new LockModel(
            GeospatialTestHelper.randomLowerCaseString(),
            GeospatialTestHelper.randomLowerCaseString(),
            Instant.now(),
            LOCK_DURATION_IN_SECONDS,
            false
        );

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            // Verifying
            assertTrue(actionRequest instanceof UpdateRequest);
            return new UpdateResponse(
                mock(ShardId.class),
                GeospatialTestHelper.randomLowerCaseString(),
                randomPositiveLong(),
                randomPositiveLong(),
                randomPositiveLong(),
                DocWriteResponse.Result.UPDATED
            );
        });

        AtomicReference<LockModel> reference = new AtomicReference<>(lockModel);
        ip2GeoLockService.getRenewLockRunnable(reference).run();
        assertEquals(lockModel, reference.get());
    }

    public void testGetRenewLockRunnable_whenLockIsStale_thenRenew() {
        LockModel lockModel = new LockModel(
            GeospatialTestHelper.randomLowerCaseString(),
            GeospatialTestHelper.randomLowerCaseString(),
            Instant.now().minusSeconds(RENEW_AFTER_IN_SECONDS),
            LOCK_DURATION_IN_SECONDS,
            false
        );

        verifyingClient.setExecuteVerifier((actionResponse, actionRequest) -> {
            // Verifying
            assertTrue(actionRequest instanceof UpdateRequest);
            return new UpdateResponse(
                mock(ShardId.class),
                GeospatialTestHelper.randomLowerCaseString(),
                randomPositiveLong(),
                randomPositiveLong(),
                randomPositiveLong(),
                DocWriteResponse.Result.UPDATED
            );
        });

        AtomicReference<LockModel> reference = new AtomicReference<>(lockModel);
        ip2GeoLockService.getRenewLockRunnable(reference).run();
        assertNotEquals(lockModel, reference.get());
    }
}
