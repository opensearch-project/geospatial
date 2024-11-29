/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.geospatial.ip2geo.listener.Ip2GeoListener;
import org.opensearch.geospatial.plugin.GeospatialPlugin;

/**
 * This class is needed for ClusterSettingsHelper.createMockNode to instantiate a test instance of the
 * GeospatialPlugin without the JobSchedulerPlugin installed. Without overriding this class, the
 * GeospatialPlugin would try to Inject JobScheduler's LockService in the GuiceHolder which will
 * fail because JobScheduler is not installed
 */
public class TestGeospatialPlugin extends GeospatialPlugin {
    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        final List<Class<? extends LifecycleComponent>> services = new ArrayList<>(1);
        services.add(Ip2GeoListener.class);
        return services;
    }
}
