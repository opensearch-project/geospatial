/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Objects;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentSubParser;
import org.opensearch.common.xcontent.support.MapXContentParser;
import org.opensearch.geometry.ShapeType;

/**
 * Parse the value and set XYPoint represented as a String, Object, WKT, array.
 */
public class XYPointParser {
    private static final String ERR_MSG_INVALID_TOKEN = "token [{}] not allowed";
    private static final String ERR_MSG_INVALID_FIELDS = "field must be either [x|y], or [type|coordinates]";
    private static final String X_PARAMETER = "x";
    private static final String Y_PARAMETER = "y";
    public static final String GEOJSON_TYPE = "type";
    public static final String GEOJSON_COORDS = "coordinates";
    private static final String NULL_VALUE_PARAMETER = "null_value";
    private static final Boolean TRUE = true;

    /**
     * Parses the value and set XYPoint which was represented as an object.
     *
     * @param value  input which needs to be parsed which contains x and y coordinates in object form
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws OpenSearchParseException
     */
    public static XYPoint parseXYPoint(Object value, final boolean ignoreZValue) throws OpenSearchParseException {
        Objects.requireNonNull(value, "input value which needs to be parsed should not be null");

        try (
            XContentParser parser = new MapXContentParser(
                NamedXContentRegistry.EMPTY,
                LoggingDeprecationHandler.INSTANCE,
                Collections.singletonMap(NULL_VALUE_PARAMETER, value),
                null
            )
        ) {
            parser.nextToken(); // start object
            parser.nextToken(); // field name
            parser.nextToken(); // field value
            return parseXYPoint(parser, ignoreZValue);
        } catch (IOException ex) {
            throw new OpenSearchParseException("error parsing xy_point", ex);
        }
    }

    /**
     * Parse the values to set the XYPoint which was represented as a String, Object, WKT, Array, or GeoJson.
     * <ul>
     *     <li>Object: <pre>{@code {"x": <x>, "y": <y}}</pre></li>
     *     <li>String: <pre>{@code "<x>,<y>"}</pre></li>
     *     <li>WKT: <pre>{@code "POINT (<x> <y>)"}</pre></li>
     *     <li>Array: <pre>{@code [<x>, <y>]}</pre></li>
     *     <li>GeoJson: <pre>{@code {"type": "Point", "coordinates": [<x>, <y>]}}</pre><li>
     * </ul>
     *
     * @param parser  {@link XContentParser} to parse the value from
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws IOException
     * @throws OpenSearchParseException
     */
    public static XYPoint parseXYPoint(XContentParser parser, final boolean ignoreZValue) throws IOException, OpenSearchParseException {
        Objects.requireNonNull(parser, "parser should not be null");
        XYPoint point = new XYPoint();
        switch (parser.currentToken()) {
            case START_OBJECT:
                parseXYPointObject(parser, point, ignoreZValue);
                break;
            case START_ARRAY:
                parseXYPointArray(parser, point, ignoreZValue);
                break;
            case VALUE_STRING:
                String val = parser.text();
                point.resetFromString(val, ignoreZValue);
                break;
            default:
                throw new OpenSearchParseException("xy_point expected");
        }
        return point;
    }

    private static XYPoint parseXYPointObject(final XContentParser parser, final XYPoint point, final boolean ignoreZValue)
        throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            if (subParser.nextToken() != XContentParser.Token.FIELD_NAME) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_TOKEN, subParser.currentToken());
            }

            String field = subParser.currentName();
            if (X_PARAMETER.equals(field) || Y_PARAMETER.equals(field)) {
                parseGeoPointObjectBasicFields(subParser, point);
            } else if (GEOJSON_TYPE.equals(field) || GEOJSON_COORDS.equals(field)) {
                parseGeoJsonFields(subParser, point, ignoreZValue);
            } else {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }

            if (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }

            return point;
        }
    }

    private static XYPoint parseGeoPointObjectBasicFields(final XContentParser parser, final XYPoint point) throws IOException {
        final int numberOfFields = 2;
        HashMap<String, Double> data = new HashMap<>();
        for (int i = 0; i < numberOfFields; i++) {
            if (i != 0) {
                parser.nextToken();
            }

            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                break;
            }

            String field = parser.currentName();
            if (X_PARAMETER.equals(field) == false && Y_PARAMETER.equals(field) == false) {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }
            switch (parser.nextToken()) {
                case VALUE_NUMBER:
                case VALUE_STRING:
                    try {
                        data.put(field, parser.doubleValue(true));
                    } catch (NumberFormatException e) {
                        throw new OpenSearchParseException("[{}] and [{}] must be valid double values", e, X_PARAMETER, Y_PARAMETER);
                    }
                    break;
                default:
                    throw new OpenSearchParseException("{} must be a number", field);
            }
        }

        if (data.get(X_PARAMETER) == null) {
            throw new OpenSearchParseException("field [{}] missing", X_PARAMETER);
        }
        if (data.get(Y_PARAMETER) == null) {
            throw new OpenSearchParseException("field [{}] missing", Y_PARAMETER);
        }

        return point.reset(data.get(X_PARAMETER), data.get(Y_PARAMETER));
    }

    private static XYPoint parseGeoJsonFields(final XContentParser parser, final XYPoint point, final boolean ignoreZValue)
        throws IOException {
        final int numberOfFields = 2;
        boolean hasTypePoint = false;
        boolean hasCoordinates = false;
        for (int i = 0; i < numberOfFields; i++) {
            if (i != 0) {
                parser.nextToken();
            }

            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                if (hasTypePoint == false) {
                    throw new OpenSearchParseException("field [{}] missing", GEOJSON_TYPE);
                }
                if (hasCoordinates == false) {
                    throw new OpenSearchParseException("field [{}] missing", GEOJSON_COORDS);
                }
            }

            if (GEOJSON_TYPE.equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw new OpenSearchParseException("{} must be a string", GEOJSON_TYPE);
                }

                // To be consistent with geo_shape parsing, ignore case here as well.
                if (ShapeType.POINT.name().equalsIgnoreCase(parser.text()) == false) {
                    throw new OpenSearchParseException("{} must be Point", GEOJSON_TYPE);
                }
                hasTypePoint = true;
            } else if (GEOJSON_COORDS.equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
                    throw new OpenSearchParseException("{} must be an array", GEOJSON_COORDS);
                }
                parseXYPointArray(parser, point, ignoreZValue);
                hasCoordinates = true;
            } else {
                throw new OpenSearchParseException(ERR_MSG_INVALID_FIELDS);
            }
        }

        return point;
    }

    private static XYPoint parseXYPointArray(final XContentParser parser, final XYPoint point, final boolean ignoreZValue)
        throws IOException {
        try (XContentSubParser subParser = new XContentSubParser(parser)) {
            double x = Double.NaN;
            double y = Double.NaN;

            int element = 0;
            while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
                if (parser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                    throw new OpenSearchParseException("numeric value expected");
                }
                element++;
                if (element == 1) {
                    x = parser.doubleValue();
                } else if (element == 2) {
                    y = parser.doubleValue();
                } else if (element == 3) {
                    GeoPoint.assertZValue(ignoreZValue, parser.doubleValue());
                } else {
                    throw new OpenSearchParseException("[xy_point] field type does not accept more than 3 values");
                }
            }

            if (element < 2) {
                throw new OpenSearchParseException("[xy_point] field type should have at least two dimensions");
            }
            return point.reset(x, y);
        }
    }
}
