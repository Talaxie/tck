/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
@Documentation("Service de construction de schéma Avro à partir d'un schéma Talend pour l'écriture Parquet.")
public class ParquetOutputSchemaService implements Serializable {

    public Schema buildAvroSchema(final org.talend.sdk.component.api.record.Schema talendSchema) {

        final List<Field> fields = new ArrayList<>();

        for (Entry entry : talendSchema.getEntries()) {
            Schema fieldSchema = toAvroType(entry);

            // Tous les champs sont NULLABLE
            Schema nullableSchema = Schema.createUnion(
                    Arrays.asList(
                            Schema.create(Schema.Type.NULL),
                            fieldSchema
                    )
            );

            Field field = new Field(
                    entry.getName(),
                    nullableSchema,
                    null,
                    Schema.NULL_VALUE
            );

            fields.add(field);
        }

        return Schema.createRecord(
                "TalaxieParquetRecord",
                "Schéma Avro généré à partir d'un Schema Talend pour l'écriture Parquet.",
                "com.talaxie.parquet",
                false,
                fields
        );
    }

    private Schema toAvroType(final Entry entry) {

        Schema baseSchema;

        switch (entry.getType()) {
            case INT:
                // INT32 = type primitif Avro
                baseSchema = Schema.create(Schema.Type.INT);
                break;

            case LONG:
                // INT64 = type primitif Avro
                baseSchema = Schema.create(Schema.Type.LONG);
                break;

            case FLOAT:
                baseSchema = Schema.create(Schema.Type.FLOAT);
                break;

            case DOUBLE:
                baseSchema = Schema.create(Schema.Type.DOUBLE);
                break;

            case BOOLEAN:
                baseSchema = Schema.create(Schema.Type.BOOLEAN);
                break;

            case BYTES:
                baseSchema = Schema.create(Schema.Type.BYTES);
                break;

            case DATETIME:
                // DATETIME → timestamp-millis
                baseSchema = LogicalTypes.timestampMillis().addToSchema(
                        Schema.create(Schema.Type.LONG)
                );
                break;

            case DECIMAL:
                Schema decimalSchema = Schema.create(Schema.Type.BYTES);
                LogicalTypes.decimal(38, 18).addToSchema(decimalSchema);
                baseSchema = decimalSchema;
                break;

            case STRING:
            default:
                baseSchema = Schema.create(Schema.Type.STRING);
                break;
        }

        return baseSchema;
    }
}