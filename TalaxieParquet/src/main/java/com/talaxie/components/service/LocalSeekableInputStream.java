/*
 * Développé par : Hervé Ciaravolo / Red-belt
 */
package com.talaxie.components.service;

import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class LocalSeekableInputStream extends SeekableInputStream {

    private final FileChannel channel;

    public LocalSeekableInputStream(FileChannel channel) {
        this.channel = channel;
    }

    @Override
    public long getPos() throws IOException {
        return channel.position();
    }

    @Override
    public void seek(long newPos) throws IOException {
        channel.position(newPos);
    }

    @Override
    public int read() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        int read = channel.read(buf);
        return read < 0 ? -1 : (buf.get(0) & 0xFF);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return channel.read(ByteBuffer.wrap(bytes, off, len));
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        readFully(bytes, 0, bytes.length);
    }

    @Override
    public void readFully(byte[] bytes, int off, int len) throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(bytes, off, len);
        while (buf.hasRemaining()) {
            if (channel.read(buf) < 0) {
                throw new IOException("Unexpected EOF");
            }
        }
    }

    @Override
    public void readFully(ByteBuffer dst) throws IOException {
        while (dst.hasRemaining()) {
            if (channel.read(dst) < 0) {
                throw new IOException("Unexpected EOF");
            }
        }
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}