/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.geospatial.settings;

import org.opensearch.common.settings.Setting;

public final class GeospatialSettings {

    // Max number of coordinates allowed in geometries while parsing uploaded GeoJSON
    public static final Setting<Integer> MAX_COORDINATES_PER_GEOMETRY = Setting.intSetting(
        "plugins.geospatial.geojson.max_coordinates_per_geo",
        10_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Max number of holes allowed in geometries while parsing uploaded GeoJSON
    public static final Setting<Integer> MAX_HOLES_PER_POLYGON = Setting.intSetting(
        "plugins.geospatial.geojson.max_holes_per_polygon",
        1_000,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Max number of multi geometries allowed while parsing uploaded GeoJSON
    public static final Setting<Integer> MAX_MULTI_GEOMETRIES = Setting.intSetting(
        "plugins.geospatial.geojson.max_multi_gemoetries",
        100,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    // Max nested depth for multi geometry structures while parsing uploaded GeoJSON
    public static final Setting<Integer> MAX_GEOMETRY_COLLECTION_NESTED_DEPTH = Setting.intSetting(
        "plugins.geospatial.geojson.max_geometry_collection_nested_depth",
        5,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
}
