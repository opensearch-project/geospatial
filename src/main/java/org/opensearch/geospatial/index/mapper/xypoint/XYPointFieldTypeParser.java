/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.util.Map;

import org.opensearch.index.mapper.AbstractPointGeometryFieldMapper;

/**
 * XYPointFieldTypeParser is used to parse and validate mapping parameters
 *
 * @author Naveen Tatikonda
 */
public class XYPointFieldTypeParser extends AbstractPointGeometryFieldMapper.TypeParser {
    @Override
    protected AbstractPointGeometryFieldMapper.Builder newBuilder(String name, Map params) {
        return new XYPointFieldMapper.XYPointFieldMapperBuilder(name);
    }

    @Override
    protected XYPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
        return XYPointParser.parseXYPoint(nullValue, ignoreZValue, new XYPoint());
    }

}
