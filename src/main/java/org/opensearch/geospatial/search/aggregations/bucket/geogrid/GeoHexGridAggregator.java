/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoGridAggregator;
import org.opensearch.geo.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

/**
 * Aggregates data expressed as H3 Cell ID.
 */
public class GeoHexGridAggregator extends GeoGridAggregator<GeoHexGrid> {

    public GeoHexGridAggregator(
        String name,
        AggregatorFactories factories,
        ValuesSource.Numeric valuesSource,
        int requiredSize,
        int shardSize,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, valuesSource, requiredSize, shardSize, aggregationContext, parent, cardinality, metadata);
    }

    @Override
    protected GeoHexGrid buildAggregation(
        String name,
        int requiredSize,
        List<InternalGeoGridBucket> buckets,
        Map<String, Object> metadata
    ) {
        return new GeoHexGrid(name, requiredSize, buckets, metadata);
    }

    @Override
    protected InternalGeoGridBucket newEmptyBucket() {
        return new GeoHexGridBucket(0, 0, null);
    }
}
