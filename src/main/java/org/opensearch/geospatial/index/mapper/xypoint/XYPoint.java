/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.opensearch.geometry.Point;
import org.opensearch.index.mapper.AbstractPointGeometryFieldMapper;

/**
 * Represents a point in a 2-dimensional planar coordinate system with no range limitations
 */
@AllArgsConstructor
@Getter
@NoArgsConstructor
public class XYPoint implements AbstractPointGeometryFieldMapper.ParsedPoint {
    private double x;
    private double y;

    /**
     * To set x and y values
     * @param x x coordinate value
     * @param y y coordinate value
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
     * @param x x coordinate
     * @param y y coordinate
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
}
