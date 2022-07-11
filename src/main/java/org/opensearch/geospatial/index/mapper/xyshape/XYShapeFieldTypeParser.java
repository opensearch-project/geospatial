/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

import java.util.Map;

import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;

/**
 * XYShapeFieldTypeParser to parse and validate mapping parameters
 */
public final class XYShapeFieldTypeParser extends AbstractShapeGeometryFieldMapper.TypeParser {
    @Override
    protected AbstractShapeGeometryFieldMapper.Builder newBuilder(String name, Map<String, Object> params) {
        return new XYShapeFieldMapper.XYShapeFieldMapperBuilder(name);
    }
}
