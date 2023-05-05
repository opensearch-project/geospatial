/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.ip2geo.common;

import static org.mockito.Mockito.mock;

import java.time.Instant;

import org.junit.Before;
import org.opensearch.action.ActionListener;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.ip2geo.Ip2GeoTestCase;

public class Ip2GeoLockServiceTests extends Ip2GeoTestCase {
    private Ip2GeoLockService ip2GeoLockService;

    @Before
    public void init() {
        ip2GeoLockService = new Ip2GeoLockService(clusterService, client);
    }

    public void testAcquireLock_whenValidInput_thenSucceed() {
        // Cannot test because LockService is final class
        // Simply calling method to increase coverage
        ip2GeoLockService.acquireLock(GeospatialTestHelper.randomLowerCaseString(), randomPositiveLong(), mock(ActionListener.class));
    }

    public void testReleaseLock_whenValidInput_thenSucceed() {
        // Cannot test because LockService is final class
        // Simply calling method to increase coverage
        ip2GeoLockService.releaseLock(null, mock(ActionListener.class));
    }

    public void testRenewLock_whenCalled_thenNotBlocked() {
        long timeoutInMillis = 10000;
        long expectedDurationInMillis = 1000;
        Instant before = Instant.now();
        assertNull(ip2GeoLockService.renewLock(null, TimeValue.timeValueMillis(timeoutInMillis)));
        Instant after = Instant.now();
        assertTrue(after.toEpochMilli() - before.toEpochMilli() < expectedDurationInMillis);
    }
}
