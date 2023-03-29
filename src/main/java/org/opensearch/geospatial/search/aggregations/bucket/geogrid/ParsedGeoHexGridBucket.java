/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import java.io.IOException;

import lombok.NoArgsConstructor;

import org.opensearch.common.geo.GeoPoint;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geo.search.aggregations.bucket.geogrid.ParsedGeoGridBucket;

@NoArgsConstructor
public class ParsedGeoHexGridBucket extends ParsedGeoGridBucket {

    public GeoPoint getKey() {
        return GeoHexHelper.h3ToGeoPoint(this.hashAsString);
    }

    public String getKeyAsString() {
        return this.hashAsString;
    }

    static ParsedGeoHexGridBucket fromXContent(XContentParser parser) throws IOException {
        return parseXContent(parser, false, ParsedGeoHexGridBucket::new, (p, bucket) -> { bucket.hashAsString = p.textOrNull(); });
    }
}
