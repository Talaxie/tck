/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import com.talaxie.components.dataset.ParquetInputDataset;
import org.apache.avro.LogicalType;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Builder;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import javax.inject.Inject;

@Service
public class ParquetInputSchemaService {

    @Inject
    private RecordBuilderFactory recordBuilderFactory;

    @DiscoverSchema
    public Schema guessSchema(final ParquetInputDataset dataset) {
        final String path = dataset.getPath();

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new Path(path))
                .withConf(new Configuration())
                .build()) {

            final GenericRecord first = reader.read();
            if (first == null) {
                throw new IllegalStateException("Fichier Parquet vide : " + path);
            }

            // Schéma Avro récupéré sur le premier enregistrement
            final org.apache.avro.Schema avroSchema = first.getSchema();
            return convert(avroSchema);

        } catch (Exception e) {
            throw new IllegalStateException("Erreur lors de la découverte du schéma Parquet : " + path, e);
        }
    }

    private org.apache.avro.Schema unwrapUnion(final org.apache.avro.Schema s) {
        if (s.getType() == org.apache.avro.Schema.Type.UNION) {
            return s.getTypes().stream()
                    .filter(t -> t.getType() != org.apache.avro.Schema.Type.NULL)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("Union null-only non supporté"));
        }
        return s;
    }

    private Type mapType(final org.apache.avro.Schema schema) {
        final org.apache.avro.Schema.Type avroType = schema.getType();
        final LogicalType logicalType = schema.getLogicalType();

        if (logicalType != null) {
            if (logicalType.getName().equals("date")) {
                return Type.DATETIME; // Talend: Date is represented as DateTime
            }
            if (logicalType.getName().startsWith("timestamp")) {
                return Type.DATETIME;
            }
            // autres Cas spécialisés si besoin (time-millis, decimal, etc.)
        }

        return switch (avroType) {
            case BOOLEAN -> Type.BOOLEAN;
            case INT -> Type.INT;
            case LONG -> Type.LONG;
            case FLOAT -> Type.FLOAT;
            case DOUBLE -> Type.DOUBLE;
            case STRING -> Type.STRING;
            case BYTES -> Type.BYTES;
            case RECORD -> Type.RECORD;
            case ARRAY -> Type.ARRAY;
            case MAP -> Type.RECORD; // fallback simple
            default -> Type.STRING;
        };
    }

    private Schema convert(final org.apache.avro.Schema avroSchema) {

        final Builder rootBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);

        for (org.apache.avro.Schema.Field field : avroSchema.getFields()) {

            final org.apache.avro.Schema effective = unwrapUnion(field.schema());
            final Type talendType = mapType(effective);

            Schema.Entry.Builder entryBuilder =
                    recordBuilderFactory
                            .newEntryBuilder()
                            .withName(field.name())
                            .withType(talendType)
                            .withNullable(isNullable(field.schema()));

            // Types primitifs → direct
            switch (talendType) {

                case STRING, INT, LONG, DOUBLE, FLOAT, BOOLEAN, BYTES -> {
                    // rien à ajouter
                }
                case ARRAY -> {
                    // On suppose array simple (list primitives ou strings)
                    org.apache.avro.Schema elementSchema = effective.getElementType();
                    Type elementType = mapType(elementSchema);
                    Schema elementTalendSchema =
                            recordBuilderFactory.newSchemaBuilder(elementType).build();
                    entryBuilder = entryBuilder.withElementSchema(elementTalendSchema);
                }

                case RECORD -> {
                    // Nested records NOT SUPPORTED -> fallback STRING
                    entryBuilder = entryBuilder.withType(Type.STRING);
                }

                default -> {
                    entryBuilder = entryBuilder.withType(Type.STRING);
                }
            }

            rootBuilder.withEntry(entryBuilder.build());
        }

        return rootBuilder.build();
    }

    private boolean isNullable(org.apache.avro.Schema s) {
        if (s.getType() == org.apache.avro.Schema.Type.UNION) {
            return s.getTypes().stream().anyMatch(t -> t.getType() == org.apache.avro.Schema.Type.NULL);
        }
        return false;
    }
}