/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Represents a point in a 2-dimensional planar coordinate system with no range limitations
 */
@AllArgsConstructor
public class XYPoint {
    @Getter
    private double x;
    @Getter
    private double y;

    public XYPoint() {}

    public XYPoint reset(double x, double y) {
        this.x = x;
        this.y = y;
        return this;
    }
}
