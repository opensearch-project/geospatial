/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xyshape;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.document.ShapeField;
import org.apache.lucene.index.IndexableField;
import org.opensearch.common.geo.GeoShapeType;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.index.mapper.AbstractShapeGeometryFieldMapper;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapperTestCase2;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.plugins.Plugin;

public class XYShapeFieldMapperTests extends FieldMapperTestCase2<XYShapeFieldMapper.XYShapeFieldMapperBuilder> {

    public static final String FIELD_TYPE_NAME = "type";
    public static final String FIELD_NAME = "field";
    public static final String COORDINATES_KEY = "coordinates";

    @Override
    protected Set<String> unsupportedProperties() {
        return Set.of("analyzer", "similarity", "doc_values", "store");
    }

    @Override
    protected void minimalMapping(XContentBuilder builder) throws IOException {
        builder.field(FIELD_TYPE_NAME, XYShapeFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected void writeFieldValue(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.value("POINT (14.0 15.0)");
    }

    @Override
    protected void registerParameters(ParameterChecker parameterChecker) throws IOException {
        parameterChecker.registerUpdateCheck(
            builder -> builder.field(
                AbstractShapeGeometryFieldMapper.Names.ORIENTATION.getPreferredName(),
                ShapeBuilder.Orientation.CLOCKWISE.name()
            ),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYShapeFieldMapper);
                XYShapeFieldMapper XYShapeFieldMapper = (XYShapeFieldMapper) mapper;
                assertEquals("param [ orientation ] is not updated", ShapeBuilder.Orientation.CLOCKWISE, XYShapeFieldMapper.orientation());
            }
        );
        parameterChecker.registerUpdateCheck(
            b -> b.field(AbstractShapeGeometryFieldMapper.Names.IGNORE_MALFORMED.getPreferredName(), true),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYShapeFieldMapper);
                XYShapeFieldMapper XYShapeFieldMapper = (XYShapeFieldMapper) mapper;
                assertTrue("param [ ignore_malformed ] is not updated", XYShapeFieldMapper.ignoreMalformed().value());
            }
        );
        parameterChecker.registerUpdateCheck(
            b -> b.field(AbstractShapeGeometryFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), false),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYShapeFieldMapper);
                XYShapeFieldMapper XYShapeFieldMapper = (XYShapeFieldMapper) mapper;
                assertFalse("param [ ignore_z_value ] is not updated", XYShapeFieldMapper.ignoreZValue().value());
            }
        );
        parameterChecker.registerUpdateCheck(
            b -> b.field(AbstractShapeGeometryFieldMapper.Names.COERCE.getPreferredName(), true),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYShapeFieldMapper);
                XYShapeFieldMapper XYShapeFieldMapper = (XYShapeFieldMapper) mapper;
                assertTrue("param [ coerce ] is not updated", XYShapeFieldMapper.coerce().value());
            }
        );
    }

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new GeospatialPlugin());
    }

    @Override
    protected boolean supportsMeta() {
        return false;
    }

    @Override
    protected boolean supportsOrIgnoresBoost() {
        return false;
    }

    public void testIndexGeoJSONPointAsShapeValue() throws IOException {
        float[] coordinates = new float[] { randomFloat(), randomFloat() };
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(
                builder -> builder.startObject(FIELD_NAME)
                    .field(COORDINATES_KEY, coordinates)
                    .field(FIELD_TYPE_NAME, GeoShapeType.POINT.shapeName())
                    .endObject()
            )
        );
        final IndexableField actualFieldValue = doc.rootDoc().getField(FIELD_NAME);
        assertNotNull(actualFieldValue);
        assertTrue("Invalid indexable field is found", actualFieldValue instanceof ShapeField.Triangle);
    }

    public void testWKTPointAsShapeValue() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(builder -> builder.field(FIELD_NAME, "POINT (100.0 -180.10)")));
        final IndexableField actualFieldValue = doc.rootDoc().getField(FIELD_NAME);
        assertNotNull(actualFieldValue);
        assertTrue("Invalid indexable field is found", actualFieldValue instanceof ShapeField.Triangle);
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertTrue("invalid mapper retrieved", fieldMapper instanceof XYShapeFieldMapper);
        XYShapeFieldMapper XYShapeFieldMapper = (XYShapeFieldMapper) fieldMapper;
        assertEquals(
            "param [ orientation ] default value should be CCW",
            XYShapeFieldMapper.fieldType().orientation(),
            AbstractShapeGeometryFieldMapper.Defaults.ORIENTATION.value()
        );
        assertEquals("param [ docs_value ] default value should be false", XYShapeFieldMapper.fieldType().hasDocValues(), false);
        assertEquals("param [ ignore_malformed ] default value should be false", XYShapeFieldMapper.ignoreMalformed().value(), false);
        assertEquals("param [ ignore_z_value ] default value should be true", XYShapeFieldMapper.ignoreZValue().value(), true);
        assertEquals("param [ coerce ] default value should be false", XYShapeFieldMapper.coerce().value(), false);
    }

    public void testFieldTypeContentType() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertTrue(fieldMapper instanceof XYShapeFieldMapper);
        final XYShapeFieldMapper.XYShapeFieldType fieldType = ((XYShapeFieldMapper) fieldMapper).fieldType();
        assertEquals("invalid field type name", fieldType.typeName(), XYShapeFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected XYShapeFieldMapper.XYShapeFieldMapperBuilder newBuilder() {
        return new XYShapeFieldMapper.XYShapeFieldMapperBuilder(GeospatialTestHelper.randomLowerCaseString());
    }
}
