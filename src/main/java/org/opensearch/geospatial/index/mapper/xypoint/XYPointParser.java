/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentSubParser;
import org.opensearch.common.xcontent.support.MapXContentParser;

/**
 * Parse the value and set XYPoint represented as a String, Object, WKT, array.
 */
public class XYPointParser {
    private static final String X_PARAMETER = "x";
    private static final String Y_PARAMETER = "y";
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
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws IOException
     * @throws OpenSearchParseException
     */
    public static XYPoint parseXYPoint(XContentParser parser, final boolean ignoreZValue) throws IOException, OpenSearchParseException {
        Objects.requireNonNull(parser, "parser should not be null");

        XYPoint point = new XYPoint();
        double x = Double.NaN;
        double y = Double.NaN;

        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            try (XContentSubParser subParser = new XContentSubParser(parser)) {
                while (subParser.nextToken() != XContentParser.Token.END_OBJECT) {
                    if (subParser.currentToken() != XContentParser.Token.FIELD_NAME) {
                        throw new OpenSearchParseException("token [{}] not allowed", subParser.currentToken());
                    }
                    String field = subParser.currentName();
                    if (!(X_PARAMETER.equals(field) || Y_PARAMETER.equals(field))) {
                        throw new OpenSearchParseException("field must be either [{}] or [{}]", X_PARAMETER, Y_PARAMETER);
                    }
                    if (X_PARAMETER.equals(field)) {
                        subParser.nextToken();
                        switch (subParser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                try {
                                    x = subParser.doubleValue(TRUE);
                                } catch (NumberFormatException numberFormatException) {
                                    throw new OpenSearchParseException("[x] must be valid double value", numberFormatException);
                                }
                                break;
                            default:
                                throw new OpenSearchParseException("[x] must be a number");
                        }
                    }
                    if (Y_PARAMETER.equals(field)) {
                        subParser.nextToken();
                        switch (subParser.currentToken()) {
                            case VALUE_NUMBER:
                            case VALUE_STRING:
                                try {
                                    y = subParser.doubleValue(TRUE);
                                } catch (NumberFormatException numberFormatException) {
                                    throw new OpenSearchParseException("[y] must be valid double value", numberFormatException);
                                }
                                break;
                            default:
                                throw new OpenSearchParseException("[y] must be a number");
                        }
                    }
                }
            }
            if (Double.isNaN(x)) {
                throw new OpenSearchParseException("field [{}] missing", X_PARAMETER);
            }
            if (Double.isNaN(y)) {
                throw new OpenSearchParseException("field [{}] missing", Y_PARAMETER);
            }
            return point.reset(x, y);
        }

        if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            return parseXYPointArray(parser, ignoreZValue, x, y);
        }

        if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            String val = parser.text();
            return point.resetFromString(val, ignoreZValue);
        }
        throw new OpenSearchParseException("Expected xy_point. But, the provided mapping is not of type xy_point");
    }

    /**
     * Parse the values to set the XYPoint which was represented as an array.
     *
     * @param subParser  {@link XContentParser} to parse the values from an array
     * @param ignoreZValue  boolean parameter which decides if third coordinate needs to be ignored or not
     * @param x  x coordinate that will be set by parsing the value from array
     * @param y  y coordinate that will be set by parsing the value from array
     * @return {@link XYPoint} after setting the x and y coordinates parsed from the parse
     * @throws IOException
     */
    private static XYPoint parseXYPointArray(XContentParser subParser, final boolean ignoreZValue, double x, double y) throws IOException {
        XYPoint point = new XYPoint();
        int element = 0;
        while (subParser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (subParser.currentToken() != XContentParser.Token.VALUE_NUMBER) {
                throw new OpenSearchParseException("numeric value expected");
            }
            element++;
            if (element == 1) {
                x = subParser.doubleValue();
            } else if (element == 2) {
                y = subParser.doubleValue();
            } else if (element == 3) {
                XYPoint.assertZValue(ignoreZValue, subParser.doubleValue());
            } else {
                throw new OpenSearchParseException("[xy_point] field type does not accept more than 3 dimensions");
            }
        }
        return point.reset(x, y);
    }
}
