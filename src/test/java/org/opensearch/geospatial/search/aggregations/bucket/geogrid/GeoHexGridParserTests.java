/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.search.aggregations.bucket.geogrid;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.opensearch.geospatial.GeospatialTestHelper.randomHexGridPrecision;
import static org.opensearch.geospatial.GeospatialTestHelper.randomLowerCaseString;
import static org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder.NAME;
import static org.opensearch.geospatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder.PARSER;

import java.util.Locale;

import org.hamcrest.MatcherAssert;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.geo.GeometryTestUtils;
import org.opensearch.geometry.Rectangle;
import org.opensearch.geospatial.h3.H3;
import org.opensearch.test.OpenSearchTestCase;

public class GeoHexGridParserTests extends OpenSearchTestCase {

    private final static int MAX_SIZE = 100;
    private final static int MIN_SIZE = 1;
    private final static int MAX_SHARD_SIZE = 100;
    private final static int MIN_SHARD_SIZE = 1;

    public void testParseValidFromInts() throws Exception {
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            buildAggregation(
                randomLowerCaseString(),
                randomHexGridPrecision(),
                randomIntBetween(MIN_SIZE, MAX_SIZE),
                randomIntBetween(MIN_SHARD_SIZE, MAX_SHARD_SIZE)
            )
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(PARSER.parse(stParser, NAME));
    }

    public void testParseValidFromStrings() throws Exception {
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            buildAggregation(
                randomLowerCaseString(),
                randomHexGridPrecision(),
                randomIntBetween(MIN_SIZE, MAX_SIZE),
                randomIntBetween(MIN_SHARD_SIZE, MAX_SHARD_SIZE)
            )
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(PARSER.parse(stParser, NAME));
    }

    public void testParseInvalidUnitPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\": \"10kg\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException ex = expectThrows(XContentParseException.class, () -> PARSER.parse(stParser, NAME));
        MatcherAssert.assertThat(ex.getMessage(), containsString("failed to parse field [precision]"));
        MatcherAssert.assertThat(ex.getCause(), instanceOf(NumberFormatException.class));
        assertEquals("For input string: \"10kg\"", ex.getCause().getMessage());
    }

    public void testParseErrorOnBooleanPrecision() throws Exception {
        XContentParser stParser = createParser(JsonXContent.jsonXContent, "{\"field\":\"my_loc\", \"precision\":false}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        XContentParseException e = expectThrows(XContentParseException.class, () -> PARSER.parse(stParser, NAME));
        MatcherAssert.assertThat(e.getMessage(), containsString("precision doesn't support values of type: VALUE_BOOLEAN"));
    }

    public void testParseErrorOnPrecisionOutOfRange() throws Exception {
        int invalidPrecision = H3.MAX_H3_RES + 1;
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            buildAggregation(
                randomLowerCaseString(),
                invalidPrecision,
                randomIntBetween(MIN_SIZE, MAX_SIZE),
                randomIntBetween(MIN_SHARD_SIZE, MAX_SHARD_SIZE)
            )
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        try {
            PARSER.parse(stParser, NAME);
            fail();
        } catch (XContentParseException ex) {
            MatcherAssert.assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals(
                String.format(
                    Locale.ROOT,
                    "Invalid precision of %d . Must be between %d and %d.",
                    invalidPrecision,
                    H3.MIN_H3_RES,
                    H3.MAX_H3_RES
                ),
                ex.getCause().getMessage()
            );
        }
    }

    public void testParseValidBounds() throws Exception {
        Rectangle bbox = GeometryTestUtils.randomRectangle();
        XContentParser stParser = createParser(
            JsonXContent.jsonXContent,
            String.format(
                Locale.ROOT,
                "{\"field\":\"my_loc\", \"precision\": 5, \"size\": 500, \"shard_size\": 550,\"bounds\": { \"top\": %s,\"bottom\": %s,\"left\": %s,\"right\": %s}}",
                bbox.getMaxY(),
                bbox.getMinY(),
                bbox.getMinX(),
                bbox.getMaxX()
            )
        );
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        // can create a factory
        assertNotNull(PARSER.parse(stParser, NAME));
    }

    private String buildAggregation(String fieldName, int precision, int size, int shardSize) {
        return String.format(
            Locale.ROOT,
            "{\"field\":\"%s\", \"precision\":%d, \"size\": %d, \"shard_size\": %d}",
            fieldName,
            precision,
            size,
            shardSize
        );
    }
}
