/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.geo.search.aggregations.bucket.geogrid.BaseGeoGrid;
import org.opensearch.geo.search.aggregations.bucket.geogrid.BaseGeoGridBucket;
import org.opensearch.search.aggregations.InternalAggregations;

/**
 * Represents a grid of cells where each cell's location is determined by a h3 cell address.
 * All h3CellAddress in a grid are of the same precision
 */
public final class GeoHexGrid extends BaseGeoGrid<GeoHexGridBucket> {

    public GeoHexGrid(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public BaseGeoGrid create(List<BaseGeoGridBucket> list) {
        return new GeoHexGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    public BaseGeoGridBucket createBucket(InternalAggregations internalAggregations, BaseGeoGridBucket baseGeoGridBucket) {
        return new GeoHexGridBucket(baseGeoGridBucket.hashAsLong(), baseGeoGridBucket.getDocCount(), internalAggregations);
    }

    @Override
    public String getWriteableName() {
        return GeoHexGridAggregationBuilder.NAME;
    }

    protected GeoHexGrid(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata) {
        super(name, requiredSize, buckets, metadata);
    }

    @Override
    protected Reader<GeoHexGridBucket> getBucketReader() {
        return GeoHexGridBucket::new;
    }

    @Override
    protected BaseGeoGrid create(String name, int requiredSize, List<BaseGeoGridBucket> buckets, Map<String, Object> metadata) {
        return new GeoHexGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    protected GeoHexGridBucket createBucket(long address, long docCount, InternalAggregations internalAggregations) {
        return new GeoHexGridBucket(address, docCount, internalAggregations);
    }

    int getRequiredSize() {
        return requiredSize;
    }
}
