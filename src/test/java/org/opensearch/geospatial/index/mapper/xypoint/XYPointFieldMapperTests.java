/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.geospatial.index.mapper.xypoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.lucene.index.IndexableField;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.geospatial.GeospatialTestHelper;
import org.opensearch.geospatial.plugin.GeospatialPlugin;
import org.opensearch.index.mapper.AbstractPointGeometryFieldMapper;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.FieldMapperTestCase2;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.plugins.Plugin;

public class XYPointFieldMapperTests extends FieldMapperTestCase2<XYPointFieldMapper.XYPointFieldMapperBuilder> {

    private static final String FIELD_TYPE_NAME = "type";
    private static final String FIELD_NAME = "field";
    private static final String FIELD_X_KEY = "x";
    private static final String FIELD_Y_KEY = "y";
    private final static Integer MIN_NUM_POINTS = 1;
    private final static Integer MAX_NUM_POINTS = 10;

    @Override
    protected XYPointFieldMapper.XYPointFieldMapperBuilder newBuilder() {
        return new XYPointFieldMapper.XYPointFieldMapperBuilder(GeospatialTestHelper.randomLowerCaseString());
    }

    @Override
    protected void minimalMapping(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.field(FIELD_TYPE_NAME, XYPointFieldMapper.CONTENT_TYPE);
    }

    @Override
    protected void writeFieldValue(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.value("POINT (14.0 15.0)");
    }

    @Override
    protected void registerParameters(ParameterChecker parameterChecker) throws IOException {
        parameterChecker.registerUpdateCheck(
            b -> b.field(AbstractPointGeometryFieldMapper.Names.IGNORE_MALFORMED.getPreferredName(), true),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYPointFieldMapper);
                XYPointFieldMapper xyPointFieldMapper = (XYPointFieldMapper) mapper;
                assertTrue("param [ ignore_malformed ] is not updated", xyPointFieldMapper.ignoreMalformed().value());
            }
        );
        parameterChecker.registerUpdateCheck(
            b -> b.field(AbstractPointGeometryFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), false),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYPointFieldMapper);
                XYPointFieldMapper xyPointFieldMapper = (XYPointFieldMapper) mapper;
                assertFalse("param [ ignore_z_value ] is not updated", xyPointFieldMapper.ignoreZValue().value());
            }
        );

        XYPoint point = new XYPoint();
        String pointAsString = "23.35,-50.55";
        point.resetFromString(pointAsString, true);
        parameterChecker.registerUpdateCheck(
            b -> b.field(AbstractPointGeometryFieldMapper.Names.NULL_VALUE.getPreferredName(), pointAsString),
            mapper -> {
                assertTrue("invalid mapper retrieved", mapper instanceof XYPointFieldMapper);
                XYPointFieldMapper xyPointFieldMapper = (XYPointFieldMapper) mapper;
                assertEquals("param [ null_value ] is not updated", point, xyPointFieldMapper.nullValue());
            }
        );
    }

    @Override
    protected Set<String> unsupportedProperties() {
        return org.opensearch.common.collect.Set.of("analyzer", "similarity");
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

    public final void testExistsQueryDocValuesDisabled() throws IOException {
        MapperService mapperService = createMapperService(fieldMapping(builder -> {
            minimalMapping(builder);
            builder.field("doc_values", false);
        }));
        assertExistsQuery(mapperService);
        assertParseMinimalWarnings();
    }

    public void testDefaultConfiguration() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertTrue("Invalid FieldMapper retrieved", fieldMapper instanceof XYPointFieldMapper);
        XYPointFieldMapper xyPointFieldMapper = (XYPointFieldMapper) fieldMapper;

        assertTrue("param [ docs_value ] default value should be true", xyPointFieldMapper.fieldType().hasDocValues());
        assertEquals("param [ ignore_malformed ] default value should be false", xyPointFieldMapper.ignoreMalformed().value(), false);
        assertEquals("param [ ignore_z_value ] default value should be true", xyPointFieldMapper.ignoreZValue().value(), true);
        assertNull("param [ null_value ] default value should be null", xyPointFieldMapper.nullValue());

    }

    public void testFieldTypeContentType() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);
        assertTrue("Invalid FieldMapper retrieved", fieldMapper instanceof XYPointFieldMapper);
        final XYPointFieldMapper.XYPointFieldType fieldType = ((XYPointFieldMapper) fieldMapper).fieldType();
        assertEquals("invalid field type name", fieldType.typeName(), XYPointFieldMapper.CONTENT_TYPE);
    }

    public void testIndexAsWKT() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(builder -> builder.field(FIELD_NAME, "POINT (" + randomDouble() + " " + randomDouble() + ")"))
        );
        final IndexableField actualFieldValue = doc.rootDoc().getField(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValue);
    }

    public void testIndexAsObject() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(
                builder -> builder.startObject(FIELD_NAME).field(FIELD_X_KEY, randomDouble()).field(FIELD_Y_KEY, randomDouble()).endObject()
            )
        );
        final IndexableField[] actualFieldValues = doc.rootDoc().getFields(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValues);
        assertEquals("mismatch in field values count", 2, actualFieldValues.length);
    }

    public void testIndexAsArray() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(
            source(builder -> builder.startArray(FIELD_NAME).value(randomDouble()).value(randomDouble()).endArray())
        );
        final IndexableField[] actualFieldValues = doc.rootDoc().getFields(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValues);
        assertEquals("mismatch in field values count", 2, actualFieldValues.length);
    }

    public void testIndexAsString() throws IOException {
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(builder -> builder.field(FIELD_NAME, randomDouble() + "," + randomDouble())));
        final IndexableField actualFieldValue = doc.rootDoc().getField(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValue);
    }

    public void testIndexAsArrayMultiPoints() throws IOException {
        int numOfPoints = randomIntBetween(MIN_NUM_POINTS, MAX_NUM_POINTS);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(builder -> {
            builder.startArray(FIELD_NAME);
            for (int i = 0; i < numOfPoints; i++) {
                builder.startArray().value(randomDouble()).value(randomDouble()).endArray();
            }
            builder.endArray();
        }));
        final IndexableField[] actualFieldValues = doc.rootDoc().getFields(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValues);
        assertEquals("mismatch in field values count", 2 * numOfPoints, actualFieldValues.length);
    }

    public void testIndexAsObjectMultiPoints() throws IOException {
        int numOfPoints = randomIntBetween(MIN_NUM_POINTS, MAX_NUM_POINTS);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(builder -> {
            builder.startArray(FIELD_NAME);
            for (int i = 0; i < numOfPoints; i++) {
                builder.startObject().field(FIELD_X_KEY, randomDouble()).field(FIELD_Y_KEY, randomDouble()).endObject();
            }
            builder.endArray();
        }));
        final IndexableField[] actualFieldValues = doc.rootDoc().getFields(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValues);
        assertEquals("mismatch in field values count", 2 * numOfPoints, actualFieldValues.length);
    }

    public void testIndexAsStringMultiPoints() throws IOException {
        int numOfPoints = randomIntBetween(MIN_NUM_POINTS, MAX_NUM_POINTS);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(builder -> {
            builder.startArray(FIELD_NAME);
            for (int i = 0; i < numOfPoints; i++) {
                builder.value(randomDouble() + "," + randomDouble());
            }
            builder.endArray();
        }));

        final IndexableField[] actualFieldValues = doc.rootDoc().getFields(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValues);
        assertEquals("mismatch in field values count", 2 * numOfPoints, actualFieldValues.length);
    }

    public void testIndexAsWKTMultiPoints() throws IOException {
        int numOfPoints = randomIntBetween(MIN_NUM_POINTS, MAX_NUM_POINTS);
        DocumentMapper mapper = createDocumentMapper(fieldMapping(this::minimalMapping));
        ParsedDocument doc = mapper.parse(source(builder -> {
            builder.startArray(FIELD_NAME);
            for (int i = 0; i < numOfPoints; i++) {
                builder.value("POINT (" + randomDouble() + " " + randomDouble() + ")");
            }
            builder.endArray();
        }));
        final IndexableField[] actualFieldValues = doc.rootDoc().getFields(FIELD_NAME);
        assertNotNull("FieldValue is null", actualFieldValues);
        assertEquals("mismatch in field values count", 2 * numOfPoints, actualFieldValues.length);
    }

    public void testIgnoreZValue() throws IOException {
        boolean z_value = randomBoolean();
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                builder -> builder.field(FIELD_TYPE_NAME, XYPointFieldMapper.CONTENT_TYPE)
                    .field(AbstractPointGeometryFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName(), z_value)
            )
        );
        if (z_value) {
            ParsedDocument doc = mapper.parse(
                source(builder -> builder.field(FIELD_NAME, randomDouble() + "," + randomDouble() + "," + randomDouble()))
            );
            final IndexableField actualFieldValue = doc.rootDoc().getField(FIELD_NAME);
            assertNotNull("failed to ignore z value even if [ignore_z_value] is [true]", actualFieldValue);
        } else {
            Exception exception = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(
                    source(builder -> builder.field(FIELD_NAME, randomDouble() + "," + randomDouble() + "," + randomDouble()))
                )
            );
            assertTrue(
                "failed to throw exception even if [ignore_z_value] is false",
                exception.getCause().getMessage().contains("but [ignore_z_value] parameter is [false]")
            );
        }
    }

    public void testIgnoreMalformed() throws IOException {
        boolean ignore_malformed_value = randomBoolean();
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                builder -> builder.field(FIELD_TYPE_NAME, XYPointFieldMapper.CONTENT_TYPE)
                    .field(AbstractPointGeometryFieldMapper.Names.IGNORE_MALFORMED.getPreferredName(), ignore_malformed_value)
            )
        );
        if (ignore_malformed_value) {
            ParsedDocument doc = mapper.parse(source(builder -> builder.field(FIELD_NAME, "50.0,abcd")));
            assertNull("failed to ignore malformed point even if [ignore_malformed] is [true]", doc.rootDoc().getField(FIELD_NAME));
        } else {
            MapperParsingException exception = expectThrows(
                MapperParsingException.class,
                () -> mapper.parse(source(builder -> builder.field(FIELD_NAME, "50.0,abcd")))
            );
            assertTrue(
                "failed to throw exception even if [ignore_malformed] is [false]",
                exception.getCause().getMessage().contains("y must be a number")
            );
        }
    }

    public void testNullValue() throws Exception {
        DocumentMapper mapper = createDocumentMapper(
            fieldMapping(
                builder -> builder.field(FIELD_TYPE_NAME, XYPointFieldMapper.CONTENT_TYPE)
                    .field(AbstractPointGeometryFieldMapper.Names.NULL_VALUE.getPreferredName(), "91,181")
            )
        );
        Mapper fieldMapper = mapper.mappers().getMapper(FIELD_NAME);

        AbstractPointGeometryFieldMapper.ParsedPoint nullValue = ((XYPointFieldMapper) fieldMapper).nullValue();
        assertEquals("assertion failed even if [null_value] parameter is set", nullValue, new XYPoint(91, 181));
    }

}
