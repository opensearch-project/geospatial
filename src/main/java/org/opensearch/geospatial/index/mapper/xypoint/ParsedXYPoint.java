/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import org.opensearch.geometry.Point;
import org.opensearch.index.mapper.AbstractPointGeometryFieldMapper;

/**
 * represents a XYPoint that has been parsed by {@link AbstractPointGeometryFieldMapper.PointParser}
 */
public class ParsedXYPoint extends XYPoint implements AbstractPointGeometryFieldMapper.ParsedPoint {

    @Override
    public void validate(String fieldName) {
        // validation is not required for xy_point
    }

    @Override
    public void normalize(String fieldName) {
        // normalization is not required for xy_point
    }

    @Override
    public void resetCoords(double x, double y) {
        this.reset(x, y);
    }

    @Override
    public Point asGeometry() {
        return new Point(getX(), getY());
    }
}
