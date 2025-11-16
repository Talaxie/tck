/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import org.apache.parquet.VersionParser.ParsedVersion;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.PrimitiveConverter;

import java.util.HashMap;
import java.util.Map;

public final class ParquetUtils {

    private ParquetUtils() {
    }

    public static Map<String, ColumnReader> buildColumnReaders(
            org.apache.parquet.schema.MessageType schema,
            PageReadStore rowGroup) {

        if (rowGroup == null) {
            return Map.of();
        }

        Map<String, ColumnReader> readers = new HashMap<>();

        for (ColumnDescriptor col : schema.getColumns()) {
            PageReader pageReader = rowGroup.getPageReader(col);
            if (pageReader == null) {
                continue;
            }

            PrimitiveConverter noop = new PrimitiveConverter() { };
            ParsedVersion version = new ParsedVersion("1", "0", "0");

            ColumnReader reader = new ColumnReaderImpl(
                    col,
                    pageReader,
                    noop,
                    version
            );

            readers.put(col.getPath()[0], reader);
        }

        return readers;
    }
}