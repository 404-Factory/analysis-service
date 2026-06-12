package com.factory.analysis_service.support;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.ByteArrayOutputStream;

/**
 * 테스트에서 Parquet 바이트를 Hadoop FileSystem(winutils 의존) 없이 메모리에 직접 쓰기 위한 OutputFile.
 * 프로덕션 읽기 경로({@code InMemoryInputFile})를 디스크 없이 검증하는 데 사용한다.
 */
public class InMemoryOutputFile implements OutputFile {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        return stream();
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return stream();
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return 0;
    }

    public byte[] toByteArray() {
        return baos.toByteArray();
    }

    private PositionOutputStream stream() {
        return new PositionOutputStream() {
            private long pos = 0;

            @Override
            public long getPos() {
                return pos;
            }

            @Override
            public void write(int b) {
                baos.write(b);
                pos++;
            }

            @Override
            public void write(byte[] b, int off, int len) {
                baos.write(b, off, len);
                pos += len;
            }
        };
    }
}
