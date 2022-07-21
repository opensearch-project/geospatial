/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.io.IOException;
import java.util.Locale;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.geo.GeoPoint;
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
 *
 * @author Naveen Tatikonda
 */
@AllArgsConstructor
@Getter
@NoArgsConstructor
public class XYPoint implements AbstractPointGeometryFieldMapper.ParsedPoint, ToXContentFragment {
    private double x;
    private double y;

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
        if (value.toLowerCase(Locale.ROOT).contains("point")) {
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
     */
    public XYPoint resetFromCoordinates(String value, final boolean ignoreZValue) {
        String[] values = value.split(",");
        final double x;
        final double y;
        int numOfValues = values.length;

        if (numOfValues > 3) {
            throw new OpenSearchParseException("expected 2 or 3 coordinates, but found: [{}]", values.length);
        }

        if (numOfValues > 2) {
            GeoPoint.assertZValue(ignoreZValue, Double.parseDouble(values[2].trim()));
        }

        try {
            x = Double.parseDouble(values[0].trim());
        } catch (NumberFormatException ex) {
            throw new OpenSearchParseException("x must be a number");
        }
        try {
            y = Double.parseDouble(values[1].trim());
        } catch (NumberFormatException ex) {
            throw new OpenSearchParseException("y must be a number");
        }

        return reset(x, y);
    }

    /**
     * Set x and y coordinates of XYPoint if input contains WKT POINT.
     *
     * @param value  input String which contains WKT POINT that needs to be parsed and validated
     * @param ignoreZValue  boolean parameter which decides if z coordinate needs to be ignored or not
     * @return XYPoint after setting the x and y coordinates
     */
    private XYPoint resetFromWKT(String value, boolean ignoreZValue) {
        Geometry geometry;
        try {
            geometry = new WellKnownText(false, new StandardValidator(ignoreZValue)).fromWKT(value);
        } catch (Exception e) {
            throw new OpenSearchParseException("Invalid WKT format", e);
        }
        if (geometry.type() != ShapeType.POINT) {
            throw new OpenSearchParseException("[xy_point] supports only POINT among WKT primitives, but found [{}]", geometry.type());
        }
        Point point = (Point) geometry;
        return reset(point.getY(), point.getX());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XYPoint)) return false;
        XYPoint point = (XYPoint) o;
        return point.x == x && point.y == y;
    }

    @Override
    public int hashCode() {
        int result = Double.hashCode(x);
        result = 31 * result + Double.hashCode(y);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Point(");
        sb.append(x);
        sb.append(",");
        sb.append(y);
        sb.append(')');
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("x", x).field("y", y).endObject();
    }
}
