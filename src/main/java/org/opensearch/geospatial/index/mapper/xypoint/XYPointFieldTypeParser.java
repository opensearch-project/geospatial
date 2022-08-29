/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.util.Map;

import org.opensearch.index.mapper.AbstractPointGeometryFieldMapper;

/**
 * XYPointFieldTypeParser is used to parse and validate mapping parameters
 */
public class XYPointFieldTypeParser extends AbstractPointGeometryFieldMapper.TypeParser {
    /**
     * Invoke XYPointFieldMapperBuilder constructor and return object.
     *
     * @param name  field name
     * @param params  parameters
     * @return invoked XYPointFieldMapperBuilder object
     */
    @Override
    protected AbstractPointGeometryFieldMapper.Builder newBuilder(String name, Map params) {
        return new XYPointFieldMapper.XYPointFieldMapperBuilder(name);
    }

    /**
     * Parse nullValue and reset XYPoint.
     *
     * @param nullValue  null_value parameter value used as a substitute for any explicit null values
     * @param ignoreZValue  if true (default), third dimension is ignored. If false, points containing more than two dimension throw an exception
     * @param ignoreMalformed  if true, malformed points are ignored else. If false(default) malformed points throw an exception
     * @return XYPoint after parsing null_value and resetting coordinates
     */
    @Override
    protected XYPoint parseNullValue(Object nullValue, boolean ignoreZValue, boolean ignoreMalformed) {
        return XYPointParser.parseXYPoint(nullValue, ignoreZValue);
    }

}
