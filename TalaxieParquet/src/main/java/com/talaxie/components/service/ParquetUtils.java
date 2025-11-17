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
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public final class ParquetUtils {

    private ParquetUtils() {
    }

    public static boolean isDecimal(ColumnDescriptor desc) {
        return desc.getType() == PrimitiveType.PrimitiveTypeName.BINARY
                && desc.getPrimitiveType().getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
    }

    public static boolean isTimestamp(ColumnDescriptor desc) {
        return desc.getPrimitiveType().getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
    }

    public static BigDecimal binaryToBigDecimal(byte[] bytes) {
        return new BigDecimal(new BigInteger(bytes), 2); // adapter scale
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