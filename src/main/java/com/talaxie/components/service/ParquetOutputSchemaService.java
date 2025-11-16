/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.Service;


@Service
public class ParquetOutputSchemaService {

    public Schema buildAvroSchema(org.talend.sdk.component.api.record.Schema talendSchema) {

        SchemaBuilder.FieldAssembler<Schema> fields =
                SchemaBuilder.record("TalaxieRecord")
                        .namespace("com.talaxie.parquet")
                        .fields();

        for (Entry e : talendSchema.getEntries()) {
            fields = fields.name(e.getName())
                    .type(convertType(e))
                    .noDefault();
        }

        return fields.endRecord();
    }

    private Schema convertType(Entry e) {

        Type t = e.getType();

        return switch (t) {

            case BOOLEAN -> SchemaBuilder.builder().booleanType();
            case INT -> SchemaBuilder.builder().intType();
            case LONG -> SchemaBuilder.builder().longType();
            case FLOAT -> SchemaBuilder.builder().floatType();
            case DOUBLE -> SchemaBuilder.builder().doubleType();
            case STRING -> SchemaBuilder.builder().stringType();

            case BYTES -> SchemaBuilder.builder().bytesType();

            case ARRAY -> SchemaBuilder.array().items(
                    SchemaBuilder.builder().stringType()
            );

            case RECORD -> {
                SchemaBuilder.FieldAssembler<Schema> nested =
                        SchemaBuilder.record(e.getName()).fields();
                e.getElementSchema().getEntries().forEach(child -> {
                    nested.name(child.getName()).type(convertType(child)).noDefault();
                });
                yield nested.endRecord();
            }

            default -> SchemaBuilder.builder().stringType();
        };
    }
}