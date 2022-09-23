/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.io.IOException;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

public class XYPointParsingTests extends OpenSearchTestCase {
    private static final String FIELD_X_KEY = "x";
    private static final String FIELD_Y_KEY = "y";

    public void testXYPointReset() {
        double x = randomDouble();
        double y = randomDouble();

        XYPoint point = new XYPoint();

        assertEquals("Reset from WKT", point.resetFromString("POINT(" + y + " " + x + ")", randomBoolean()), point.reset(x, y));
        assertEquals("Reset from Coordinates", point.resetFromString(x + ", " + y, randomBoolean()), point.reset(x, y));

    }

    public void testResetFromWKTInvalid() {
        XYPoint point = new XYPoint();
        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> point.resetFromString("NOT A POINT(1 2)", randomBoolean())
        );
        assertEquals("Validation failed for Invalid WKT", "Invalid WKT format, [NOT A POINT(1 2)]", e.getMessage());

        OpenSearchParseException e2 = expectThrows(
            OpenSearchParseException.class,
            () -> point.resetFromString("MULTIPOINT(1 2, 3 4)", randomBoolean())
        );
        assertEquals(
            "Validation failed for invalid WKT primitive",
            "[xy_point] supports only POINT among WKT primitives, but found [MULTIPOINT]",
            e2.getMessage()
        );
    }

    public void testResetFromCoordinatesInvalid() {
        XYPoint point = new XYPoint();
        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> point.resetFromString("20.4, 50.6, 70.8, -200.6", randomBoolean())
        );
        assertEquals("Validation failed for checking count of coordinates", "expected 2 or 3 coordinates, but found: [4]", e.getMessage());

        OpenSearchParseException e2 = expectThrows(OpenSearchParseException.class, () -> point.resetFromString("20.4, 50.6, 70.8", false));
        assertEquals(
            "Validation failed for [ignore_z_value] parameter",
            "Exception parsing coordinates: found Z value [70.8] but [ignore_z_value] parameter is [false]",
            e2.getMessage()
        );

        OpenSearchParseException e3 = expectThrows(
            OpenSearchParseException.class,
            () -> point.resetFromString("abcd, 50.6", randomBoolean())
        );
        assertEquals("Validation failed even if x is not a number", "[x] must be a number", e3.getMessage());

        OpenSearchParseException e4 = expectThrows(
            OpenSearchParseException.class,
            () -> point.resetFromString("50.6, abcd", randomBoolean())
        );
        assertEquals("Validation failed even if y is not a number", "[y] must be a number", e4.getMessage());
    }

    public void testXYPointParsing() throws IOException {
        XYPoint randomXYPoint = new XYPoint(randomDouble(), randomDouble());

        XYPoint point = XYPointParser.parseXYPoint(xyAsObject(randomXYPoint.getX(), randomXYPoint.getY()), randomBoolean());
        assertEquals("Parsing XYPoint as an object failed", randomXYPoint, point);

        XYPoint point2 = XYPointParser.parseXYPoint(xyAsString(randomXYPoint.getX(), randomXYPoint.getY()), randomBoolean());
        assertEquals("Parsing XYPoint as a string failed", randomXYPoint, point2);

        XYPoint point3 = XYPointParser.parseXYPoint(xyAsArray(randomXYPoint.getX(), randomXYPoint.getY()), randomBoolean());
        assertEquals("Parsing XYPoint as an array failed", randomXYPoint, point3);

        XYPoint point4 = XYPointParser.parseXYPoint(xyAsWKT(randomXYPoint.getX(), randomXYPoint.getY()), randomBoolean());
        assertEquals("Parsing XYPoint as a WKT failed", randomXYPoint, point4);
    }

    public void testInvalidField() throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field(FIELD_Y_KEY, 0).field(FIELD_X_KEY, 0).field("test", 0);
        content.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content))) {
            parser.nextToken();
            OpenSearchParseException e = expectThrows(
                OpenSearchParseException.class,
                () -> XYPointParser.parseXYPoint(parser, randomBoolean())
            );
            assertEquals("Validation for invalid fields failed", "field must be either [x] or [y]", e.getMessage());
        }
    }

    public void testParsingInvalidObject() throws IOException {
        // Send empty string instead of double for y coordinate
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field(FIELD_X_KEY, randomDouble()).field(FIELD_Y_KEY, "");
        content.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        OpenSearchParseException e = expectThrows(
            OpenSearchParseException.class,
            () -> XYPointParser.parseXYPoint(parser, randomBoolean())
        );
        assertEquals("Validation failed for invalid x and y values", "[y] must be valid double value", e.getMessage());

        // Skip the 'y' field and y coordinate
        XContentBuilder content1 = JsonXContent.contentBuilder();
        content1.startObject();
        content1.field(FIELD_X_KEY, randomDouble());
        content1.endObject();
        XContentParser parser1 = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content1));
        parser1.nextToken();
        OpenSearchParseException e1 = expectThrows(
            OpenSearchParseException.class,
            () -> XYPointParser.parseXYPoint(parser1, randomBoolean())
        );
        assertEquals("Validation failed even if field [y] is missing", "field [y] missing", e1.getMessage());

        // Skip the 'x' field and x coordinate
        XContentBuilder content2 = JsonXContent.contentBuilder();
        content2.startObject();
        content2.field(FIELD_Y_KEY, randomDouble());
        content2.endObject();
        XContentParser parser2 = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content2));
        parser2.nextToken();
        OpenSearchParseException e2 = expectThrows(
            OpenSearchParseException.class,
            () -> XYPointParser.parseXYPoint(parser2, randomBoolean())
        );
        assertEquals("Validation failed even if field [x] is missing", "field [x] missing", e2.getMessage());

    }

    private XContentParser xyAsObject(double x, double y) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startObject();
        content.field(FIELD_X_KEY, x).field(FIELD_Y_KEY, y);
        content.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }

    private XContentParser xyAsString(double x, double y) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.value(x + ", " + y);
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }

    private XContentParser xyAsArray(double x, double y) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.startArray().value(x).value(y).endArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }

    private XContentParser xyAsWKT(double x, double y) throws IOException {
        XContentBuilder content = JsonXContent.contentBuilder();
        content.value("POINT (" + x + " " + y + ")");
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(content));
        parser.nextToken();
        return parser;
    }
}
