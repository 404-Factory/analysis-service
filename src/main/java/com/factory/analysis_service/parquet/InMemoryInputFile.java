package com.factory.analysis_service.parquet;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * S3에서 받은 byte[]를 디스크 임시파일 없이 그대로 Parquet 리더에 넘기기 위한 {@link InputFile} 구현.
 *
 * <p>기존 구현은 {@code getObjectAsBytes()}로 이미 메모리에 올라온 바이트를 다시 임시파일에 쓰고
 * ({@code Files.write}) {@code LocalInputFile}로 디스크에서 재차 읽은 뒤 삭제했다.
 * Parquet은 footer seek 때문에 random-access가 필요할 뿐, 디스크가 필요한 것은 아니므로
 * 메모리 바이트 배열 위에서 seek/read를 직접 제공하여 디스크 I/O를 0으로 만든다.
 */
public class InMemoryInputFile implements InputFile {

    private final byte[] data;

    public InMemoryInputFile(byte[] data) {
        this.data = data;
    }

    @Override
    public long getLength() {
        return data.length;
    }

    @Override
    public SeekableInputStream newStream() {
        return new InMemorySeekableInputStream(data);
    }

    private static final class InMemorySeekableInputStream extends SeekableInputStream {

        private final byte[] data;
        private int pos = 0;

        private InMemorySeekableInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) throws IOException {
            if (newPos < 0 || newPos > data.length) {
                throw new IOException("Invalid seek position: " + newPos + " (length=" + data.length + ")");
            }
            pos = (int) newPos;
        }

        @Override
        public int read() {
            return pos < data.length ? (data[pos++] & 0xFF) : -1;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, n);
            pos += n;
            return n;
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            if (pos + len > data.length) {
                throw new EOFException("Reached end of stream with " + len + " bytes left to read");
            }
            System.arraycopy(data, pos, bytes, start, len);
            pos += len;
        }

        @Override
        public int read(ByteBuffer buf) {
            if (pos >= data.length) {
                return -1;
            }
            int n = Math.min(buf.remaining(), data.length - pos);
            buf.put(data, pos, n);
            pos += n;
            return n;
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            int n = buf.remaining();
            if (pos + n > data.length) {
                throw new EOFException("Reached end of stream with " + n + " bytes left to read");
            }
            buf.put(data, pos, n);
            pos += n;
        }

        @Override
        public int available() {
            return data.length - pos;
        }
    }
}
