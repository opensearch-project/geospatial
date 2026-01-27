/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.geospatial.settings;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;

import lombok.Getter;

public class GeospatialSettingsAccessor {
    @Getter
    private volatile int maxCoordinatesPerGeometry;
    @Getter
    private volatile int maxHolesPerPolygon;
    @Getter
    private volatile int maxMultiGeometries;
    @Getter
    private volatile int maxGeometryCollectionNestedDepth;

    /**
     * Constructor, registers callbacks to update settings
     * @param clusterService
     * @param settings
     */
    public GeospatialSettingsAccessor(ClusterService clusterService, Settings settings) {
        maxCoordinatesPerGeometry = GeospatialSettings.MAX_COORDINATES_PER_GEOMETRY.get(settings);
        maxHolesPerPolygon = GeospatialSettings.MAX_HOLES_PER_POLYGON.get(settings);
        maxMultiGeometries = GeospatialSettings.MAX_MULTI_GEOMETRIES.get(settings);
        maxGeometryCollectionNestedDepth = GeospatialSettings.MAX_GEOMETRY_COLLECTION_NESTED_DEPTH.get(settings);
        registerSettingsCallbacks(clusterService);
    }

    private void registerSettingsCallbacks(ClusterService clusterService) {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(GeospatialSettings.MAX_COORDINATES_PER_GEOMETRY, value -> {
            maxCoordinatesPerGeometry = value;
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(GeospatialSettings.MAX_HOLES_PER_POLYGON, value -> {
            maxHolesPerPolygon = value;
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(GeospatialSettings.MAX_MULTI_GEOMETRIES, value -> {
            maxMultiGeometries = value;
        });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(GeospatialSettings.MAX_GEOMETRY_COLLECTION_NESTED_DEPTH, value -> {
            maxGeometryCollectionNestedDepth = value;
        });
    }
}
