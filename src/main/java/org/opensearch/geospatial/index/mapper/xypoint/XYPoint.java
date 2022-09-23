/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import static org.opensearch.index.mapper.AbstractGeometryFieldMapper.Names.IGNORE_Z_VALUE;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.Point;
import org.opensearch.geometry.ShapeType;
import org.opensearch.geometry.utils.StandardValidator;
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.index.mapper.AbstractPointGeometryFieldMapper;

/**
 * Represents a point in a 2-dimensional planar coordinate system with no range limitations.
 */
@AllArgsConstructor
@Getter
@NoArgsConstructor
public class XYPoint implements AbstractPointGeometryFieldMapper.ParsedPoint, ToXContentFragment {
    private double x;
    private double y;
    private static final String POINT_PRIMITIVE = "point";
    private static final String X_PARAMETER = "x";
    private static final String Y_PARAMETER = "y";
    private static final String XY_POINT = "XY_POINT";

    /**
     * To set x and y values
     *
     * @param x  x coordinate value
     * @param y  y coordinate value
     * @return initialized XYPoint
     */
    public XYPoint reset(double x, double y) {
        this.x = x;
        this.y = y;
        return this;
    }

    @Override
    public void validate(String fieldName) {
        // validation is not required for xy_point
    }

    @Override
    public void normalize(String fieldName) {
        // normalization is not required for xy_point
    }

    /**
     * To set x and y values
     *
     * @param x  x coordinate
     * @param y  y coordinate
     */
    @Override
    public void resetCoords(double x, double y) {
        this.reset(x, y);
    }

    /**
     * @return returns geometry of type point
     */
    @Override
    public Point asGeometry() {
        return new Point(getX(), getY());
    }

    /**
     * Set x and y coordinates of XYPoint if input contains WKT or coordinates.
     *
     * @param value  input String which needs to be parsed and validated
     * @param ignoreZValue  boolean parameter which decides if z coordinate needs to be ignored or not
     * @return XYPoint after setting the x and y coordinates
     */
    public XYPoint resetFromString(String value, final boolean ignoreZValue) {
        Objects.requireNonNull(value, "input string which needs to be parsed should not be null");

        if (value.toLowerCase(Locale.ROOT).contains(POINT_PRIMITIVE)) {
            return resetFromWKT(value, ignoreZValue);
        }
        return resetFromCoordinates(value, ignoreZValue);
    }

    /**
     * Set x and y coordinates of XYPoint if input contains coordinates.
     *
     * @param value  input String which contains coordinates that needs to be parsed and validated
     * @param ignoreZValue  boolean parameter which decides if z coordinate needs to be ignored or not
     * @return XYPoint after setting the x and y coordinates
     * throws OpenSearchParseException
     */
    public XYPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        Objects.requireNonNull(value, "input string which needs to be parsed should not be null");

        String[] values = value.split(",");
        int numOfValues = values.length;

        if (numOfValues > 3) {
            throw new OpenSearchParseException("expected 2 or 3 coordinates, but found: [{}]", values.length);
        }

        if (numOfValues > 2) {
            assertZValue(ignoreZValue, Double.parseDouble(values[2].trim()));
        }

        double x = parseCoordinate(values[0], X_PARAMETER);
        double y = parseCoordinate(values[1], Y_PARAMETER);

        return reset(x, y);
    }

    /**
     * Parse and extract coordinate value from given String.
     *
     * @param value  input String which contains coordinate that needs to be parsed and validated
     * @param parameter  x or y parameter
     * @return parsed coordinate value
     * throws OpenSearchParseException
     */
    private double parseCoordinate(String value, String parameter) {
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException ex) {
            throw new OpenSearchParseException("[{}] must be a number", parameter, ex);
        }
    }

    /**
     * Set x and y coordinates of XYPoint if input contains WKT POINT.
     *
     * @param value  input String which contains WKT POINT that needs to be parsed and validated
     * @param ignoreZValue  boolean parameter which decides if z coordinate needs to be ignored or not
     * @return XYPoint after setting the x and y coordinates
     * throws OpenSearchParseException
     */
    private XYPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = new WellKnownText(false, new StandardValidator(ignoreZValue)).fromWKT(value);
        } catch (Exception e) {
            throw new OpenSearchParseException("Invalid WKT format, [{}]", value, e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new OpenSearchParseException("[xy_point] supports only POINT among WKT primitives, but found [{}]", geometry.type());
        }
        Point point = (Point) geometry;
        return reset(point.getX(), point.getY());
    }

    /**
     * Validates if z coordinate value needs to be ignored or not.
     *
     * @param ignoreZValue  boolean parameter which decides if z coordinate needs to be ignored or not
     * @param zValue  z coordinate value
     * throws OpenSearchParseException
     */
    public static void assertZValue(boolean ignoreZValue, double zValue) {
        if (!ignoreZValue) {
            throw new OpenSearchParseException(
                "Exception parsing coordinates: found Z value [{}] but [{}] parameter is [{}]",
                zValue,
                IGNORE_Z_VALUE,
                ignoreZValue
            );
        }
    }

    /**
     * Deep Comparison to compare object of XYPoint class w.r.t state of the object
     *
     * @param obj  Object
     * @return true if the parameter obj is of type XYPoint and its data members (x and y) are same, else false
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof XYPoint)) return false;
        XYPoint point = (XYPoint) obj;
        return point.x == x && point.y == y;
    }

    /**
     * This method returns the hash code value for the object on which this method is invoked.
     *
     * @return hashcode value as an Integer
     */
    @Override
    public int hashCode() {
        int result = Double.hashCode(x);
        result = 31 * result + Double.hashCode(y);
        return result;
    }

    /**
     * String representation of XYPoint object.
     *
     * @return XYPoint object as "Point(x,y)" where x and y are coordinate values
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(XY_POINT);
        sb.append('(');
        sb.append(x);
        sb.append(",");
        sb.append(y);
        sb.append(')');
        return sb.toString();
    }

    /**
     * Return x and y coordinates in the object form.
     *
     * @param builder  XContentBuilder object
     * @param params  Params object
     * @return x and y coordinates in the object form. For example: {"x" : 100.35, "y" : -200.54}
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(X_PARAMETER, x).field(Y_PARAMETER, y).endObject();
    }
}
