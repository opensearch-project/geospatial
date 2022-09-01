/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;

import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.index.IndexableField;
import org.opensearch.geometry.Point;
import org.opensearch.geospatial.index.common.xyshape.XYShapeConverter;
import org.opensearch.index.mapper.AbstractGeometryFieldMapper;
import org.opensearch.index.mapper.ParseContext;

/**
 * Converts points into Lucene-compatible form{@link XYPoint} for indexing in a xy_point field.
 */
@AllArgsConstructor
public class XYPointIndexer
    implements
        AbstractGeometryFieldMapper.Indexer<List<org.opensearch.geospatial.index.mapper.xypoint.XYPoint>, List<? extends XYPoint>> {
    private final String fieldName;

    /**
     * Converts the list of {@link org.opensearch.geospatial.index.mapper.xypoint.XYPoint} to list of {@link XYPoint}
     * @param points list of {@link org.opensearch.geospatial.index.mapper.xypoint.XYPoint}
     * @return list of {@link XYPoint} that are converted from opensearch to lucene type
     */
    @Override
    public List<? extends XYPoint> prepareForIndexing(List<org.opensearch.geospatial.index.mapper.xypoint.XYPoint> points) {
        Objects.requireNonNull(points, "XYPoint cannot be null");

        if (points.isEmpty()) {
            throw new IllegalArgumentException("XYPoint cannot be empty");
        }

        return points.stream()
            .map(parsedXYPoint -> new Point(parsedXYPoint.getX(), parsedXYPoint.getY()))
            .map(XYShapeConverter::toXYPoint)
            .collect(Collectors.toList());
    }

    /**
     * @return processed class type
     */
    @Override
    public Class<List<? extends XYPoint>> processedClass() {
        Object listToObjectClass = List.class;
        return (Class<List<? extends XYPoint>>) listToObjectClass.getClass();
    }

    /**
     * converts the List of {@link XYPoint} to list of {@link IndexableField}.
     * The {@link IndexableField} returned are of type {@link XYPointField}
     * @param context {@link ParseContext}
     * @param xyPoints {@link XYPoint} list
     * @return List of {@link IndexableField}
     */
    @Override
    public List<IndexableField> indexShape(ParseContext context, List<? extends XYPoint> xyPoints) {
        return xyPoints.stream().map(xyPoint -> new XYPointField(fieldName, xyPoint.getX(), xyPoint.getY())).collect(Collectors.toList());
    }
}
