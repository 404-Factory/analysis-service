package com.factory.analysis_service.support;

import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;

/**
 * 프로덕션이 읽는 것과 동일한 스키마(sensorType/unit/avg_value)의 Parquet 바이트를 생성하는 테스트 헬퍼.
 */
public final class ParquetTestSupport {

    public static final MessageType SCHEMA = Types.buildMessage()
            .required(BINARY).as(stringType()).named("sensorType")
            .required(BINARY).as(stringType()).named("unit")
            .required(DOUBLE).named("avg_value")
            .named("summary");

    private ParquetTestSupport() {
    }

    public record Row(String sensorType, String unit, double avgValue) {
    }

    /**
     * 주어진 행들로 압축 없는 Parquet 파일 바이트를 만든다. (UNCOMPRESSED — 네이티브 코덱 의존 제거)
     */
    public static byte[] parquetBytes(List<Row> rows) {
        InMemoryOutputFile outputFile = new InMemoryOutputFile();
        SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);

        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withType(SCHEMA)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()) {
            for (Row row : rows) {
                Group group = factory.newGroup()
                        .append("sensorType", row.sensorType())
                        .append("unit", row.unit())
                        .append("avg_value", row.avgValue());
                writer.write(group);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return outputFile.toByteArray();
    }
}
