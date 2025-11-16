/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class LocalInputFile implements InputFile {

    private final File file;

    public LocalInputFile(String path) {
        this.file = new File(path);
        if (!file.exists()) {
            throw new IllegalStateException("Parquet file not found: " + path);
        }
    }

    @Override
    public long getLength() {
        return file.length();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new LocalSeekableInputStream(new FileInputStream(file).getChannel());
    }
}