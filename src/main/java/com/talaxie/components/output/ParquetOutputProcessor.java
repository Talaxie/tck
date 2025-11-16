/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.output;

import com.talaxie.components.dataset.ParquetOutputDataset;
import com.talaxie.components.service.LocalOutputFile;
import com.talaxie.components.service.ParquetOutputSchemaService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.avro.AvroParquetWriter;

import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;


import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

@Version(1)
@Icon(value = CUSTOM, custom = "ParquetOutput")
@Processor(name = "ParquetOutput")
@Documentation("Écrit un fichier Parquet depuis un flux Record.")
public class ParquetOutputProcessor implements Serializable {

    private final ParquetOutputSchemaService schemaService;
    private final ParquetOutputConfiguration configuration;
    private transient ParquetWriter<GenericRecord> writer;
    private transient Schema avroSchema;

    public ParquetOutputProcessor(
            @Option("configuration") final ParquetOutputConfiguration configuration,
            final ParquetOutputSchemaService schemaService
    ) {
        this.configuration = configuration;
        this.schemaService = schemaService;
    }

    @PostConstruct
    public void init() {
        ParquetOutputDataset ds = configuration.getDataset();
        String path = ds.getPath();

        if (ds.isOverwrite()) {
            File f = new File(path);
            if (f.exists()) {
                boolean deleted = f.delete();

                if (!deleted) {
                    throw new IllegalStateException(
                            Messages.errorDeleteFailed(path)
                    );
                }
            }
        }
    }

    @ElementListener
    public void onNext(@Input final Record record) {
        if (record == null) {
            return;
        }

        try {
            if (writer == null) {
                initWriter(record);
            }

            GenericRecord avroRecord = new GenericData.Record(avroSchema);

            for (Entry e : record.getSchema().getEntries()) {
                Object value = record.get(Object.class, e.getName());
                avroRecord.put(e.getName(), value);
            }

            writer.write(avroRecord);

        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Erreur d'écriture Parquet", e);
        }
    }

    private void initWriter(Record firstRecord) throws IOException {
        ParquetOutputDataset ds = configuration.getDataset();
        String path = ds.getPath();
        File f = new File(path);

        // ====== Gestion overwrite / erreur ======
        if (f.exists()) {
            if (!ds.isOverwrite()) {
                // Le fichier existe, et overwrite n'est pas activé -> erreur
                throw new IllegalStateException(
                        Messages.errorFileExists(path)
                );
            }

            // overwrite = true -> on supprime
            boolean deleted = f.delete();
            if (!deleted) {
                throw new IllegalStateException(
                        Messages.errorDeleteFailed(path)
                );
            }
        }

        // ====== Création du schema ======
        avroSchema = schemaService.buildAvroSchema(firstRecord.getSchema());

        CompressionCodecName codec = switch (ds.getCompression()) {
            case GZIP -> CompressionCodecName.GZIP;
            case SNAPPY -> CompressionCodecName.SNAPPY;
            default -> CompressionCodecName.UNCOMPRESSED;
        };


        LocalOutputFile outputFile = new LocalOutputFile(path);

        writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(avroSchema)
                .withCompressionCodec(codec)
                .withRowGroupSize(configuration.getRowGroupSize())
                .withPageSize(configuration.getPageSize())
                .withDictionaryPageSize(configuration.getDictionaryPageSize())
                .withDictionaryEncoding(configuration.isEnableDictionary())
                .build();
    }

    @PreDestroy
    public void release() {
        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException ignored) {
        }
    }
}