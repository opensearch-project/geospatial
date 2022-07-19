/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import static org.opensearch.geospatial.index.common.xyshape.XYShapeConverter.toXYPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import lombok.AllArgsConstructor;

import org.apache.lucene.document.XYPointField;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.index.IndexableField;
import org.opensearch.geometry.Point;
import org.opensearch.index.mapper.AbstractGeometryFieldMapper;
import org.opensearch.index.mapper.ParseContext;

/**
 * Converts points into Lucene-compatible form for indexing in a xy_point field.
 */
@AllArgsConstructor
public class XYPointIndexer implements AbstractGeometryFieldMapper.Indexer<List<ParsedXYPoint>, List<? extends XYPoint>> {
    private final String fieldName;

    @Override
    public List<? extends XYPoint> prepareForIndexing(List<ParsedXYPoint> points) {
        Objects.requireNonNull(points, "XYPoint cannot be null");

        if (points.isEmpty()) {
            throw new IllegalArgumentException("XYPoint cannot be empty");
        }

        List<XYPoint> xyPoints = new ArrayList<>();

        for (ParsedXYPoint parsedXYPoint : points) {
            xyPoints.add(toXYPoint(new Point(parsedXYPoint.getX(), parsedXYPoint.getY())));
        }

        return xyPoints;
    }

    @Override
    public Class<List<? extends XYPoint>> processedClass() {
        return (Class<List<? extends XYPoint>>) (Object) List.class;
    }

    @Override
    public List<IndexableField> indexShape(ParseContext context, List<? extends XYPoint> xyPoints) {
        ArrayList<IndexableField> fields = new ArrayList<>(xyPoints.size());
        for (XYPoint xyPoint : xyPoints) {
            fields.add(new XYPointField(fieldName, xyPoint.getX(), xyPoint.getY()));
        }
        return fields;
    }
}
