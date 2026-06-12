package com.factory.analysis_service.service;

import com.factory.analysis_service.config.AwsS3Properties;
import com.factory.analysis_service.entity.DailySensorSummary;
import com.factory.analysis_service.parquet.InMemoryInputFile;
import com.factory.analysis_service.repository.DailySensorSummaryRepository;
import com.factory.analysis_service.dto.MonthlySummaryResponseDTO;
import com.factory.analysis_service.dto.SensorSummaryDTO;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class S3SummaryService {

    static {
        System.setProperty("hadoop.home.dir", "/");
    }

    private static final int LOOKBACK_DAYS = 30;
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final S3Client s3Client;
    private final AwsS3Properties awsS3Properties;
    private final WebClient webClient;
    private final DailySensorSummaryRepository cacheRepository;
    private final ExecutorService s3FetchExecutor;

    @Value("${services.management-service.url}")
    private String managementServiceUrl;

    @Value("${services.anomaly-service.url}")
    private String anomalyServiceUrl;

    public MonthlySummaryResponseDTO getMonthlySummary(String equipmentName) {
        LocalDate today = LocalDate.now();
        String sinceStr = today.minusDays(LOOKBACK_DAYS).atStartOfDay().toString();

        // (3) defect/anomaly count 두 호출을 S3 집계와 동시에 시작한다. (논블로킹으로 먼저 띄움)
        CompletableFuture<long[]> countsFuture = fetchCountsAsync(equipmentName, sinceStr);

        // (1)(2)(4) 캐시 조회 → 미적중 일자만 병렬 S3 조회 → 인메모리 Parquet 파싱 → 캐시 저장
        List<SensorSummaryDTO> sensors = aggregateSensors(equipmentName, today);

        long[] counts = countsFuture.join();

        return MonthlySummaryResponseDTO.builder()
                .totalDefects(counts[0])
                .totalAnomalies(counts[1])
                .sensors(sensors)
                .build();
    }

    // ----------------------------------------------------------------------------------
    // 센서 집계: 캐시 + 병렬 S3
    // ----------------------------------------------------------------------------------

    private List<SensorSummaryDTO> aggregateSensors(String equipmentName, LocalDate today) {
        List<LocalDate> dates = new ArrayList<>(LOOKBACK_DAYS);
        for (int i = 1; i <= LOOKBACK_DAYS; i++) {
            dates.add(today.minusDays(i));
        }
        LocalDate from = today.minusDays(LOOKBACK_DAYS);
        LocalDate to = today.minusDays(1);

        // (4) 캐시를 단 한 번의 쿼리로 일괄 로딩
        Map<LocalDate, List<ParquetRow>> rowsByDate = new LinkedHashMap<>();
        for (DailySensorSummary cached : cacheRepository
                .findByEquipmentIdAndSummaryDateBetween(equipmentName, from, to)) {
            rowsByDate.computeIfAbsent(cached.getSummaryDate(), k -> new ArrayList<>())
                    .add(new ParquetRow(cached.getSensorType(), cached.getUnit(), cached.getAvgValue()));
        }

        // 캐시에 없는 일자만 추려서
        List<LocalDate> missing = dates.stream()
                .filter(d -> !rowsByDate.containsKey(d))
                .collect(Collectors.toList());

        // (1) 미적중 일자들을 병렬로 S3 조회
        if (!missing.isEmpty()) {
            Map<LocalDate, List<ParquetRow>> fetched = fetchDatesInParallel(equipmentName, missing);
            rowsByDate.putAll(fetched);
            persistCache(equipmentName, fetched);
        }

        // 일자 순서대로 센서타입별 평균값 누적
        Map<String, List<Double>> avgByType = new LinkedHashMap<>();
        Map<String, String> unitByType = new LinkedHashMap<>();
        for (LocalDate date : dates) {
            List<ParquetRow> rows = rowsByDate.get(date);
            if (rows == null) {
                continue;
            }
            for (ParquetRow row : rows) {
                avgByType.computeIfAbsent(row.sensorType(), k -> new ArrayList<>()).add(row.avgValue());
                unitByType.put(row.sensorType(), row.unit());
            }
        }

        return avgByType.entrySet().stream()
                .map(e -> SensorSummaryDTO.builder()
                        .sensorType(e.getKey())
                        .unit(unitByType.get(e.getKey()))
                        .avgValue(roundTwo(e.getValue().stream()
                                .mapToDouble(Double::doubleValue).average().orElse(0.0)))
                        .build())
                .collect(Collectors.toList());
    }

    private Map<LocalDate, List<ParquetRow>> fetchDatesInParallel(String equipmentName, List<LocalDate> dates) {
        List<CompletableFuture<Map.Entry<LocalDate, List<ParquetRow>>>> futures = dates.stream()
                .map(date -> CompletableFuture.supplyAsync(
                        () -> Map.entry(date, fetchRowsByPrefix(buildPrefix(date, equipmentName))),
                        s3FetchExecutor))
                .collect(Collectors.toList());

        Map<LocalDate, List<ParquetRow>> result = new LinkedHashMap<>();
        for (CompletableFuture<Map.Entry<LocalDate, List<ParquetRow>>> future : futures) {
            Map.Entry<LocalDate, List<ParquetRow>> entry = future.join();
            // 데이터가 없는 일자는 캐싱/집계에서 제외
            if (!entry.getValue().isEmpty()) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }

    private void persistCache(String equipmentName, Map<LocalDate, List<ParquetRow>> fetched) {
        List<DailySensorSummary> toSave = new ArrayList<>();
        fetched.forEach((date, rows) -> {
            for (ParquetRow row : rows) {
                toSave.add(DailySensorSummary.builder()
                        .equipmentId(equipmentName)
                        .summaryDate(date)
                        .sensorType(row.sensorType())
                        .unit(row.unit())
                        .avgValue(row.avgValue())
                        .build());
            }
        });
        if (toSave.isEmpty()) {
            return;
        }
        try {
            cacheRepository.saveAll(toSave);
        } catch (DataAccessException e) {
            // 동시 요청이 같은 일자를 동시에 적재하려다 충돌한 경우 — 캐시일 뿐이므로 무시
            log.warn("### 일일 요약 캐시 저장 충돌(동시 요청 추정), 무시: {}", e.getMessage());
        }
    }

    private String buildPrefix(LocalDate date, String equipmentName) {
        return String.format("summary-data/date=%s/equipmentId=%s/", date.format(DATE_FMT), equipmentName);
    }

    // ----------------------------------------------------------------------------------
    // 외부 서비스 count 동시 호출
    // ----------------------------------------------------------------------------------

    private CompletableFuture<long[]> fetchCountsAsync(String equipmentName, String sinceStr) {
        Mono<Long> defects = countMono(
                managementServiceUrl + "/api/management/defects/count?equipmentName=" + equipmentName + "&since=" + sinceStr,
                "management-service");
        Mono<Long> anomalies = countMono(
                anomalyServiceUrl + "/api/anomalies/count?equipmentName=" + equipmentName + "&since=" + sinceStr,
                "anomaly-service");

        // zip 은 두 Mono 를 동시에 구독 → 두 호출이 병렬 실행된다.
        return Mono.zip(defects, anomalies, (d, a) -> new long[]{d, a}).toFuture();
    }

    private Mono<Long> countMono(String uri, String serviceName) {
        return webClient.get()
                .uri(uri)
                .retrieve()
                // ApiResponse 전체를 역직렬화하지 않고 필요한 data 필드만 추출 (역직렬화 견고성↑)
                .bodyToMono(CountResponse.class)
                .map(res -> res.data() != null ? res.data() : 0L)
                .onErrorResume(e -> {
                    log.error("### {} count API 호출 실패: {}", serviceName, e.getMessage());
                    return Mono.just(0L);
                });
    }

    /** management/anomaly count 응답에서 data 값만 받기 위한 최소 DTO. (success/status 등은 무시) */
    @JsonIgnoreProperties(ignoreUnknown = true)
    private record CountResponse(Long data) {}

    // ----------------------------------------------------------------------------------
    // S3 + Parquet (인메모리)
    // ----------------------------------------------------------------------------------

    private List<ParquetRow> fetchRowsByPrefix(String prefix) {
        try {
            List<String> keys = listParquetKeys(prefix);
            if (keys.isEmpty()) {
                return Collections.emptyList();
            }
            // 폴더(date+equipment)당 parquet 파일은 1개라는 데이터 적재 규약에 따라 첫 파일을 읽는다.
            return fetchRows(keys.get(0));
        } catch (Throwable e) {
            log.error("### prefix 조회 실패: {}", prefix, e);
            return Collections.emptyList();
        }
    }

    private List<String> listParquetKeys(String prefix) {
        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                        .bucket(awsS3Properties.getS3().getBucket())
                        .prefix(prefix)
                        .build())
                .contents().stream()
                .map(S3Object::key)
                .filter(k -> k.endsWith(".parquet"))
                .collect(Collectors.toList());
    }

    private List<ParquetRow> fetchRows(String s3Key) {
        try {
            byte[] bytes = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder()
                            .bucket(awsS3Properties.getS3().getBucket())
                            .key(s3Key)
                            .build()
            ).asByteArray();

            // (2) 임시파일 없이 메모리 바이트에서 바로 Parquet 파싱
            return readParquet(new InMemoryInputFile(bytes));
        } catch (Throwable e) {
            log.error("### S3 Parquet 읽기 실패: {}", s3Key, e);
            return Collections.emptyList();
        }
    }

    private List<ParquetRow> readParquet(InputFile inputFile) throws IOException {
        List<ParquetRow> rows = new ArrayList<>();
        try (ParquetFileReader fileReader = ParquetFileReader.open(inputFile)) {
            MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            PageReadStore pages;
            while ((pages = fileReader.readNextRowGroup()) != null) {
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                long count = pages.getRowCount();
                for (long i = 0; i < count; i++) {
                    rows.add(toRow(recordReader.read()));
                }
            }
        }
        return rows;
    }

    private ParquetRow toRow(Group g) {
        return new ParquetRow(
                g.getString("sensorType", 0),
                g.getString("unit", 0),
                g.getDouble("avg_value", 0)
        );
    }

    private double roundTwo(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    // ----------------------------------------------------------------------------------
    // 디버그 엔드포인트 (인메모리 Parquet 읽기로 통일)
    // ----------------------------------------------------------------------------------

    public Map<String, String> debugS3(String equipmentName) {
        Map<String, String> result = new LinkedHashMap<>();
        String bucket = awsS3Properties.getS3().getBucket();
        String prefix = String.format("summary-data/date=2026-05-28/equipmentId=%s/", equipmentName);

        result.put("bucket", bucket);
        result.put("region", awsS3Properties.getRegion());
        result.put("prefix", prefix);

        try {
            List<String> keys = listParquetKeys(prefix);
            if (keys.isEmpty()) {
                result.put("foundFiles", "없음");
                return result;
            }
            result.put("foundFiles", keys.get(0));

            byte[] bytes = s3Client.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucket).key(keys.get(0)).build()
            ).asByteArray();
            result.put("fileSize", bytes.length + " bytes");

            try (ParquetFileReader fileReader = ParquetFileReader.open(new InMemoryInputFile(bytes))) {
                MessageType schema = fileReader.getFooter().getFileMetaData().getSchema();
                result.put("schema", schema.toString());

                PageReadStore pages = fileReader.readNextRowGroup();
                if (pages != null) {
                    result.put("rowCount", String.valueOf(pages.getRowCount()));
                    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    RecordReader<Group> rr = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                    Group g = rr.read();
                    if (g != null) {
                        result.put("firstRow", g.toString());
                    }
                }
            }
        } catch (Throwable e) {
            result.put("error", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        return result;
    }

    private record ParquetRow(String sensorType, String unit, double avgValue) {}
}
