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

@Version(2)
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
                    throw new IllegalStateException(Messages.errorDeleteFailed(path));
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
                Object rawValue = record.get(Object.class, e.getName());
                Object converted = convertValue(rawValue, e);
                avroRecord.put(e.getName(), converted);
            }

            writer.write(avroRecord);

        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException(Messages.errorParquetWriter(), e);
        }
    }

    private Object convertValue(Object value, Entry entry) {
        if (value == null) {
            return null;
        }

        try {
            switch (entry.getType()) {
                case INT:
                    if (value instanceof Number) return ((Number) value).intValue();
                    return Integer.parseInt(value.toString());

                case LONG:
                    if (value instanceof Number) return ((Number) value).longValue();
                    return Long.parseLong(value.toString());

                case FLOAT:
                    if (value instanceof Number) return ((Number) value).floatValue();
                    return Float.parseFloat(value.toString());

                case DOUBLE:
                    if (value instanceof Number) return ((Number) value).doubleValue();
                    return Double.parseDouble(value.toString());

                case BOOLEAN:
                    return Boolean.valueOf(value.toString());

                case DATETIME:
                    if (value instanceof java.time.Instant instant) {
                        return instant.toEpochMilli();
                    }
                    if (value instanceof java.time.LocalDateTime dt) {
                        return dt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                    if (value instanceof java.time.LocalDate date) {
                        return date.atStartOfDay(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                    }
                    return null;

                case BYTES:
                    return value instanceof byte[] ? value : value.toString().getBytes();

                case STRING:
                default:
                    return value.toString();
            }
        } catch (Exception e) {
            throw e;
        }
    }

    private void initWriter(Record firstRecord) throws IOException {
        ParquetOutputDataset ds = configuration.getDataset();
        String path = ds.getPath();
        File f = new File(path);

        if (f.exists()) {
            if (!ds.isOverwrite()) {
                throw new IllegalStateException(Messages.errorFileExistsAndOverwriteDisabled(path));
            }
            boolean deleted = f.delete();
            if (!deleted) {
                throw new IllegalStateException(Messages.errorDeleteFailed(path));
            }
        }

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