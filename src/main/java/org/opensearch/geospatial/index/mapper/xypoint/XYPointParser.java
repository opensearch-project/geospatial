/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.io.IOException;
import java.util.Collections;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentSubParser;
import org.opensearch.common.xcontent.support.MapXContentParser;

/**
 * Parse the value and set XYPoint represented as a String, Object, WKT, array.
 *
 * @author Naveen Tatikonda
 */
public class XYPointParser {
    public static final String X = "x";
    public static final String Y = "y";
    public static final String NULL_VALUE_PARAMETER = "null_value";

    /**
     * Parses the value and set XYPoint which was represented as an object.
     *
     * @param value  input which needs to be parsed which contains x and y coordinates in object form
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @param point  A {@link XYPoint} that will be reset by the values parsed
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws OpenSearchParseException
     */
    public static XYPoint parseXYPoint(Object value, final boolean ignoreZValue, XYPoint point) throws OpenSearchParseException {
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
            return parseXYPoint(parser, point, ignoreZValue);
        } catch (IOException ex) {
            throw new OpenSearchParseException("error parsing xy_point", ex);
        }
    }

    /**
     * Parse the values to set the XYPoint which was represented as a String, Object, WKT or an array.
     *
     * <ul>
     *     <li> String: "100.35, -200.54" </li>
     *     <li> Object: {"x" : 100.35, "y" : -200.54} </li>
     *     <li> WKT: "POINT (-200.54 100.35)"</li>
     *     <li> Array: [ -200.54, 100.35 ]</li>
     * </ul>
     *
     * @param parser  {@link XContentParser} to parse the value from
     * @param point  A {@link XYPoint} that will be reset by the values parsed
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws IOException
     * @throws OpenSearchParseException
     */
    public static XYPoint parseXYPoint(XContentParser parser, XYPoint point, final boolean ignoreZValue) throws IOException,
        OpenSearchParseException {
        double x = Double.NaN;
        double y = Double.NaN;
        NumberFormatException numberFormatException = null;

        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                while (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (subParser.currentToken() != XContentParser.Token.FIELD_NAME) {
                        throw new OpenSearchParseException("token [{}] not allowed", subParser.currentToken());
                    }
                    String field = subParser.currentName();
                    if (!(X.equals(field) || Y.equals(field))) {
                        throw new OpenSearchParseException("field must be either [{}] or [{}]", X, Y);
                    }
                    if (X.equals(field)) {
                        subParser.nextToken();
                        switch (subParser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                try {
                                    x = subParser.doubleValue(true);
                                } catch (NumberFormatException e) {
                                    numberFormatException = e;
                                }
                                break;
                            default:
                                throw new OpenSearchParseException("x must be a number");
                        }
                    }
                    if (Y.equals(field)) {
                        subParser.nextToken();
                        switch (subParser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                try {
                                    y = subParser.doubleValue(true);
                                } catch (NumberFormatException e) {
                                    numberFormatException = e;
                                }
                                break;
                            default:
                                throw new OpenSearchParseException("y must be a number");
                        }
                    }
                }
            }
            if (numberFormatException != null) {
                throw new OpenSearchParseException("[{}] and [{}] must be valid double values", numberFormatException, X, Y);
            }
            if (Double.isNaN(x)) {
                throw new OpenSearchParseException("field [{}] missing", X);
            }
            if (Double.isNaN(y)) {
                throw new OpenSearchParseException("field [{}] missing", Y);
            }
            return point.reset(x, y);
        }

        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            return parseXYPointArray(parser, point, ignoreZValue, x, y);
        }

        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            String val = parser.text();
            return point.resetFromString(val, ignoreZValue);
        }
        throw new OpenSearchParseException("xy_point expected");
    }

    /**
     * Parse the values to set the XYPoint which was represented as an array.
     *
     * @param subParser  {@link XContentParser} to parse the values from an array
     * @param point  A {@link XYPoint} that will be reset by the values parsed
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @param x  x coordinate that will be set by parsing the value from array
     * @param y  y coordinate that will be set by parsing the value from array
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws IOException
     */
    public static XYPoint parseXYPointArray(XContentParser subParser, XYPoint point, final boolean ignoreZValue, double x, double y)
        throws IOException {
        int element = 0;
        while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (subParser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                throw new OpenSearchParseException("numeric value expected");
            }
            element++;
            if (element == 1) {
                y = subParser.doubleValue();
            } else if (element == 2) {
                x = subParser.doubleValue();
            } else if (element == 3) {
                GeoPoint.assertZValue(ignoreZValue, subParser.doubleValue());
            } else {
                throw new OpenSearchParseException("[xy_point] field type does not accept more than 3 dimensions");
            }
        }
        return point.reset(x, y);
    }
}
