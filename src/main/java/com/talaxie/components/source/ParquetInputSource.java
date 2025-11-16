/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.source;

import com.talaxie.components.dataset.ParquetInputDataset;
import com.talaxie.components.service.LocalInputFile;
import com.talaxie.components.service.ParquetUtils;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

@Documentation("Low-level Parquet reader without Hadoop FS, compatible with Talend Studio.")
public class ParquetInputSource implements Serializable {

    private final ParquetInputMapperConfiguration configuration;
    private final RecordBuilderFactory recordBuilderFactory;

    // === Parquet structures ===
    private transient ParquetFileReader fileReader;
    private transient MessageType schema;
    private transient PageReadStore currentGroup;
    private transient Map<String, ColumnReader> columnReaders;
    private long rowsRemaining;

    public ParquetInputSource(
            @Option("configuration") final ParquetInputMapperConfiguration configuration,
            final RecordBuilderFactory recordBuilderFactory) {

        this.configuration = configuration;
        this.recordBuilderFactory = recordBuilderFactory;
    }

    // ============================================================
    // Init : ouverture du fichier et premier row group
    // ============================================================
    @PostConstruct
    public void init() {
        ParquetInputDataset dataset = configuration.getDataset();
        String path = dataset.getPath();

        try {
            LocalInputFile inputFile = new LocalInputFile(path);
            this.fileReader = ParquetFileReader.open(inputFile);

            ParquetMetadata footer = fileReader.getFooter();
            this.schema = footer.getFileMetaData().getSchema();

            loadNextRowGroup();

        } catch (IOException e) {
            throw new IllegalStateException("Unable to open Parquet file: " + path, e);
        }
    }

    private void loadNextRowGroup() throws IOException {
        this.currentGroup = fileReader.readNextRowGroup();

        if (currentGroup == null) {
            this.columnReaders = null;
            this.rowsRemaining = 0;
            return;
        }

        this.columnReaders = ParquetUtils.buildColumnReaders(schema, currentGroup);
        this.rowsRemaining = currentGroup.getRowCount();
    }

    // ============================================================
    // Lecture d'une ligne
    // ============================================================
    @Producer
    public Record next() {
        try {
            if (columnReaders == null || rowsRemaining == 0) {
                loadNextRowGroup();
                if (columnReaders == null) {
                    return null; // EOF
                }
            }

            Record.Builder builder = recordBuilderFactory.newRecordBuilder();

            for (String colName : columnReaders.keySet()) {
                ColumnReader reader = columnReaders.get(colName);

                switch (reader.getDescriptor().getType()) {
                    case BOOLEAN:
                        builder.withBoolean(colName, reader.getBoolean());
                        break;

                    case INT32:
                        builder.withInt(colName, reader.getInteger());
                        break;

                    case INT64:
                        builder.withLong(colName, reader.getLong());
                        break;

                    case FLOAT:
                        builder.withFloat(colName, reader.getFloat());
                        break;

                    case DOUBLE:
                        builder.withDouble(colName, reader.getDouble());
                        break;

                    case BINARY:
                        builder.withString(colName, reader.getBinary().toStringUsingUTF8());
                        break;

                    default:
                        builder.withString(colName, null);
                        break;
                }

                reader.consume();
            }

            rowsRemaining--;
            return builder.build();

        } catch (Exception e) {
            throw new IllegalStateException("Error reading Parquet row", e);
        }
    }

    // ============================================================
    // Cleanup
    // ============================================================
    @PreDestroy
    public void release() {
        try {
            if (fileReader != null) {
                fileReader.close();
            }
        } catch (IOException ignored) {
        }
    }
}