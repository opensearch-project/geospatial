/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import java.io.IOException;

import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geo.search.aggregations.bucket.geogrid.ParsedGeoGrid;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class ParsedGeoHexGrid extends ParsedGeoGrid {
    private static final ObjectParser<ParsedGeoGrid, Void> PARSER = createParser(
        ParsedGeoHexGrid::new,
        ParsedGeoHexGridBucket::fromXContent,
        ParsedGeoHexGridBucket::fromXContent
    );

    public static ParsedGeoGrid fromXContent(XContentParser parser, String name) throws IOException {
        final var parsedGeoGrid = PARSER.parse(parser, null);
        parsedGeoGrid.setName(name);
        return parsedGeoGrid;
    }

    public String getType() {
        return GeoHexGridAggregationBuilder.NAME;
    }
}
