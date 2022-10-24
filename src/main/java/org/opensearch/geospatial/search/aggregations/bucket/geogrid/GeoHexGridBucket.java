/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexHelper.h3ToGeoPoint;

import java.io.IOException;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.geo.search.aggregations.bucket.geogrid.BaseGeoGridBucket;
import org.opensearch.geospatial.h3.H3;
import org.opensearch.search.aggregations.InternalAggregations;

/**
 * Implementation of geohex grid bucket
 */
public class GeoHexGridBucket extends BaseGeoGridBucket<GeoHexGridBucket> {

    public GeoHexGridBucket(long hashAsLong, long docCount, InternalAggregations aggregations) {
        super(hashAsLong, docCount, aggregations);
    }

    /**
     * Read from a Stream
     * @param in {@link StreamInput} contains GridBucket
     * @throws IOException
     */
    public GeoHexGridBucket(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Object getKey() {
        return h3ToGeoPoint(hashAsLong);
    }

    @Override
    public String getKeyAsString() {
        return H3.h3ToString(hashAsLong);
    }
}
