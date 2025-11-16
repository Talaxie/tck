/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Implémentation locale d’OutputFile (sans Hadoop),
 * utilisable directement par AvroParquetWriter.
 * Pour éviter les collisions de librairies dans Talaxie
 */
public class LocalOutputFile implements OutputFile {

    private final File file;

    public LocalOutputFile(final String path) {
        this.file = new File(path);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new LocalPositionOutputStream(new FileOutputStream(file, false));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new LocalPositionOutputStream(new FileOutputStream(file, false));
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }


    // ---- Implémentation du PositionOutputStream --------------------------

    private static class LocalPositionOutputStream extends PositionOutputStream {

        private final FileOutputStream out;
        private long pos = 0;

        LocalPositionOutputStream(final FileOutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b) throws IOException {
            out.write(b);
            pos += b.length;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            pos += len;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}