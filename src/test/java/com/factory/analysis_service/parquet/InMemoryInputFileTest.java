package com.factory.analysis_service.parquet;

import com.factory.analysis_service.support.ParquetTestSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("InMemoryInputFile - 인메모리 Parquet 읽기")
class InMemoryInputFileTest {

    @Test
    @DisplayName("#5 디스크 임시파일 없이 메모리 바이트에서 Parquet 전체 행을 읽는다")
    void readsAllRowsFromMemory() throws Exception {
        byte[] bytes = ParquetTestSupport.parquetBytes(List.of(
                new ParquetTestSupport.Row("TEMP", "C", 21.5),
                new ParquetTestSupport.Row("PRESSURE", "kPa", 101.3),
                new ParquetTestSupport.Row("VIBRATION", "mm/s", 0.42)
        ));

        List<String[]> rows = readRows(bytes);

        assertThat(rows).hasSize(3);
        assertThat(rows.get(0)).containsExactly("TEMP", "C", "21.5");
        assertThat(rows.get(1)).containsExactly("PRESSURE", "kPa", "101.3");
        assertThat(rows.get(2)).containsExactly("VIBRATION", "mm/s", "0.42");
    }

    @Test
    @DisplayName("getLength는 바이트 길이를 반환한다")
    void getLengthReturnsByteCount() {
        assertThat(new InMemoryInputFile(new byte[]{1, 2, 3, 4}).getLength()).isEqualTo(4L);
    }

    @Nested
    @DisplayName("SeekableInputStream 동작")
    class StreamBehavior {

        private final byte[] data = {10, 20, 30, 40, 50};

        @Test
        @DisplayName("seek 후 getPos가 일치하고 이후 read가 해당 위치부터 읽는다")
        void seekAndRead() throws IOException {
            try (SeekableInputStream s = new InMemoryInputFile(data).newStream()) {
                s.seek(2);
                assertThat(s.getPos()).isEqualTo(2L);
                assertThat(s.read()).isEqualTo(30);
                assertThat(s.getPos()).isEqualTo(3L);
            }
        }

        @Test
        @DisplayName("EOF에서 read()는 -1, read(byte[])는 -1을 반환한다")
        void readAtEof() throws IOException {
            try (SeekableInputStream s = new InMemoryInputFile(data).newStream()) {
                s.seek(5);
                assertThat(s.read()).isEqualTo(-1);
                assertThat(s.read(new byte[4], 0, 4)).isEqualTo(-1);
                assertThat(s.read(ByteBuffer.allocate(4))).isEqualTo(-1);
            }
        }

        @Test
        @DisplayName("readFully(byte[])는 정확히 채우고, 범위를 넘으면 EOFException")
        void readFully() throws IOException {
            try (SeekableInputStream s = new InMemoryInputFile(data).newStream()) {
                byte[] buf = new byte[3];
                s.readFully(buf);
                assertThat(buf).containsExactly(10, 20, 30);
                assertThat(s.getPos()).isEqualTo(3L);

                assertThatThrownBy(() -> s.readFully(new byte[10]))
                        .isInstanceOf(IOException.class);
            }
        }

        @Test
        @DisplayName("read(ByteBuffer)와 readFully(ByteBuffer)가 위치를 전진시킨다")
        void byteBufferReads() throws IOException {
            try (SeekableInputStream s = new InMemoryInputFile(data).newStream()) {
                ByteBuffer b1 = ByteBuffer.allocate(2);
                assertThat(s.read(b1)).isEqualTo(2);
                assertThat(b1.array()).containsExactly(10, 20);

                ByteBuffer b2 = ByteBuffer.allocate(2);
                s.readFully(b2);
                assertThat(b2.array()).containsExactly(30, 40);
                assertThat(s.getPos()).isEqualTo(4L);
            }
        }

        @Test
        @DisplayName("available은 남은 바이트 수를 반환한다")
        void available() throws IOException {
            try (SeekableInputStream s = new InMemoryInputFile(data).newStream()) {
                assertThat(s.available()).isEqualTo(5);
                s.seek(3);
                assertThat(s.available()).isEqualTo(2);
            }
        }

        @Test
        @DisplayName("음수/범위초과 seek는 IOException을 던진다")
        void invalidSeek() throws IOException {
            try (SeekableInputStream s = new InMemoryInputFile(data).newStream()) {
                assertThatThrownBy(() -> s.seek(-1)).isInstanceOf(IOException.class);
                assertThatThrownBy(() -> s.seek(99)).isInstanceOf(IOException.class);
            }
        }
    }

    // ParquetFileReader로 InMemoryInputFile을 읽어 (sensorType, unit, avg_value)를 추출
    private List<String[]> readRows(byte[] bytes) throws IOException {
        List<String[]> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new InMemoryInputFile(bytes))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                RecordReader<Group> rr = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                long count = pages.getRowCount();
                for (long i = 0; i < count; i++) {
                    Group g = rr.read();
                    rows.add(new String[]{
                            g.getString("sensorType", 0),
                            g.getString("unit", 0),
                            trimDouble(g.getDouble("avg_value", 0))
                    });
                }
            }
        }
        return rows;
    }

    private String trimDouble(double v) {
        if (v == Math.rint(v)) {
            return String.valueOf((long) v);
        }
        return String.valueOf(v);
    }
}
