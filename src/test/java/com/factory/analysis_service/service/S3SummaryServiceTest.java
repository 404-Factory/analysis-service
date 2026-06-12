package com.factory.analysis_service.service;

import com.factory.analysis_service.config.AwsS3Properties;
import com.factory.analysis_service.dto.MonthlySummaryResponseDTO;
import com.factory.analysis_service.dto.SensorSummaryDTO;
import com.factory.analysis_service.entity.DailySensorSummary;
import com.factory.analysis_service.repository.DailySensorSummaryRepository;
import com.factory.analysis_service.support.ParquetTestSupport;
import com.factory.analysis_service.support.StubWebClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("S3SummaryService - 월간 요약 집계")
class S3SummaryServiceTest {

    private static final String EQUIPMENT = "EQP-DEPOSITION-001";

    private S3Client s3Client;
    private DailySensorSummaryRepository cacheRepository;
    private ExecutorService executor;
    private AwsS3Properties props;
    private S3SummaryService service;

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        cacheRepository = mock(DailySensorSummaryRepository.class);
        executor = Executors.newFixedThreadPool(4);

        props = new AwsS3Properties();
        props.getS3().setBucket("test-bucket");
        props.setRegion("ap-northeast-2");

        // 기본: 캐시 미적중
        when(cacheRepository.findByEquipmentIdAndSummaryDateBetween(any(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cacheRepository.saveAll(any())).thenAnswer(inv -> new ArrayList<>(inv.getArgument(0)));

        service = new S3SummaryService(
                s3Client, props, StubWebClients.counting(7, 3), cacheRepository, executor);
        ReflectionTestUtils.setField(service, "managementServiceUrl", "http://mgmt");
        ReflectionTestUtils.setField(service, "anomalyServiceUrl", "http://anomaly");
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    private void stubS3Returns(ParquetTestSupport.Row... rows) {
        ListObjectsV2Response listResp = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("summary-data/part-0.parquet").build())
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResp);

        byte[] bytes = ParquetTestSupport.parquetBytes(List.of(rows));
        ResponseBytes<GetObjectResponse> response =
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), bytes);
        when(s3Client.getObjectAsBytes(any(GetObjectRequest.class))).thenReturn(response);
    }

    @Test
    @DisplayName("#1 S3 Parquet에서 센서 데이터를 정상적으로 가져와 30일 평균을 집계한다")
    void aggregatesSensorAveragesFromS3() {
        // 모든 일자가 동일하게 TEMP=20.0, PRESSURE=100.0 을 반환
        stubS3Returns(
                new ParquetTestSupport.Row("TEMP", "C", 20.0),
                new ParquetTestSupport.Row("PRESSURE", "kPa", 100.0));

        MonthlySummaryResponseDTO result = service.getMonthlySummary(EQUIPMENT);

        Map<String, SensorSummaryDTO> byType = result.getSensors().stream()
                .collect(Collectors.toMap(SensorSummaryDTO::getSensorType, s -> s));
        assertThat(byType).containsOnlyKeys("TEMP", "PRESSURE");
        assertThat(byType.get("TEMP").getAvgValue()).isEqualTo(20.0);
        assertThat(byType.get("TEMP").getUnit()).isEqualTo("C");
        assertThat(byType.get("PRESSURE").getAvgValue()).isEqualTo(100.0);

        // 30일치를 모두 조회했는지 (병렬 fetch)
        verify(s3Client, times(30)).getObjectAsBytes(any(GetObjectRequest.class));
    }

    @Test
    @DisplayName("#1 여러 일자의 서로 다른 값을 올바른 평균으로 계산한다 (소수 2자리 반올림)")
    void averagesAcrossDifferentDailyValues() {
        // 병렬 실행에서도 결정적이도록, 값은 호출 순서가 아니라 '키에 박힌 날짜'로 결정한다.
        // listObjectsV2는 prefix(=날짜 포함)를 그대로 키에 실어 돌려준다.
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(inv -> {
            ListObjectsV2Request req = inv.getArgument(0);
            return ListObjectsV2Response.builder()
                    .contents(S3Object.builder().key(req.prefix() + "part.parquet").build())
                    .build();
        });
        // getObjectAsBytes는 키의 date=YYYY-MM-dd 에서 일(day-of-month)을 값으로 사용
        when(s3Client.getObjectAsBytes(any(GetObjectRequest.class))).thenAnswer(inv -> {
            GetObjectRequest req = inv.getArgument(0);
            double v = dayFromKey(req.key());
            byte[] bytes = ParquetTestSupport.parquetBytes(
                    List.of(new ParquetTestSupport.Row("TEMP", "C", v)));
            return ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), bytes);
        });

        // 기대값: 최근 30일(어제~30일 전)의 day-of-month 평균을 소수 2자리로 반올림
        LocalDate today = LocalDate.now();
        double expected = IntStream.rangeClosed(1, 30)
                .mapToDouble(i -> today.minusDays(i).getDayOfMonth())
                .average().orElse(0.0);
        expected = Math.round(expected * 100.0) / 100.0;

        MonthlySummaryResponseDTO result = service.getMonthlySummary(EQUIPMENT);

        assertThat(result.getSensors()).hasSize(1);
        assertThat(result.getSensors().get(0).getAvgValue()).isEqualTo(expected);
    }

    private static int dayFromKey(String key) {
        int idx = key.indexOf("date=");
        String date = key.substring(idx + 5, idx + 15); // YYYY-MM-dd
        return Integer.parseInt(date.substring(8, 10));
    }

    @Test
    @DisplayName("#2 management/anomaly 서비스와 통신해 defect/anomaly count를 가져온다")
    void fetchesCountsFromOtherServices() {
        stubS3Returns(new ParquetTestSupport.Row("TEMP", "C", 20.0));

        MonthlySummaryResponseDTO result = service.getMonthlySummary(EQUIPMENT);

        assertThat(result.getTotalDefects()).isEqualTo(7L);
        assertThat(result.getTotalAnomalies()).isEqualTo(3L);
    }

    @Test
    @DisplayName("#2 외부 서비스 실패/응답누락 시 count는 0으로 폴백한다")
    void fallsBackToZeroOnExternalFailure() {
        stubS3Returns(new ParquetTestSupport.Row("TEMP", "C", 20.0));
        service = new S3SummaryService(s3Client, props,
                StubWebClients.failingManagementAndNullAnomaly(), cacheRepository, executor);
        ReflectionTestUtils.setField(service, "managementServiceUrl", "http://mgmt");
        ReflectionTestUtils.setField(service, "anomalyServiceUrl", "http://anomaly");

        MonthlySummaryResponseDTO result = service.getMonthlySummary(EQUIPMENT);

        assertThat(result.getTotalDefects()).isZero();
        assertThat(result.getTotalAnomalies()).isZero();
        // 외부 호출이 실패해도 센서 집계는 정상
        assertThat(result.getSensors()).hasSize(1);
    }

    @Test
    @DisplayName("#4 S3에서 읽은 일자별 데이터를 캐시에 저장한다")
    void persistsFetchedDataToCache() {
        stubS3Returns(
                new ParquetTestSupport.Row("TEMP", "C", 20.0),
                new ParquetTestSupport.Row("PRESSURE", "kPa", 100.0));

        service.getMonthlySummary(EQUIPMENT);

        // 30일 × 2센서 = 60행 저장
        @SuppressWarnings("unchecked")
        java.util.List<DailySensorSummary> saved = captureSaved();
        assertThat(saved).hasSize(60);
        assertThat(saved).allSatisfy(row -> {
            assertThat(row.getEquipmentId()).isEqualTo(EQUIPMENT);
            assertThat(row.getSensorType()).isIn("TEMP", "PRESSURE");
        });
    }

    @SuppressWarnings("unchecked")
    private List<DailySensorSummary> captureSaved() {
        var captor = org.mockito.ArgumentCaptor.forClass(List.class);
        verify(cacheRepository).saveAll(captor.capture());
        return captor.getValue();
    }

    @Test
    @DisplayName("#4 캐시가 적중하면 S3를 조회하지 않고 캐시 값으로 집계한다")
    void usesCacheWhenPresentAndSkipsS3() {
        LocalDate today = LocalDate.now();
        List<DailySensorSummary> cached = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            cached.add(DailySensorSummary.builder()
                    .equipmentId(EQUIPMENT).summaryDate(today.minusDays(i))
                    .sensorType("TEMP").unit("C").avgValue(25.0).build());
        }
        when(cacheRepository.findByEquipmentIdAndSummaryDateBetween(any(), any(), any()))
                .thenReturn(cached);

        MonthlySummaryResponseDTO result = service.getMonthlySummary(EQUIPMENT);

        assertThat(result.getSensors()).hasSize(1);
        assertThat(result.getSensors().get(0).getAvgValue()).isEqualTo(25.0);
        // 전부 캐시 적중 → S3 호출 없음, 저장도 없음
        verify(s3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        verify(s3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
        verify(cacheRepository, never()).saveAll(any());
    }

    @Test
    @DisplayName("S3 prefix에 parquet이 없으면 해당 일자는 건너뛴다")
    void skipsDatesWithoutParquet() {
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().contents(Collections.emptyList()).build());

        MonthlySummaryResponseDTO result = service.getMonthlySummary(EQUIPMENT);

        assertThat(result.getSensors()).isEmpty();
        verify(s3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
        // 저장할 것도 없음
        verify(cacheRepository, never()).saveAll(any());
    }

    @Test
    @DisplayName("debugS3는 파일을 찾으면 스키마/행수/첫행을 진단 맵에 담는다")
    void debugS3ReturnsDiagnostics() {
        stubS3Returns(new ParquetTestSupport.Row("TEMP", "C", 20.0));

        Map<String, String> result = service.debugS3(EQUIPMENT);

        assertThat(result.get("bucket")).isEqualTo("test-bucket");
        assertThat(result.get("region")).isEqualTo("ap-northeast-2");
        assertThat(result.get("foundFiles")).isNotBlank();
        assertThat(result.get("rowCount")).isEqualTo("1");
        assertThat(result).containsKey("schema");
        assertThat(result).containsKey("firstRow");
    }

    @Test
    @DisplayName("debugS3는 parquet이 없으면 foundFiles=없음을 반환한다")
    void debugS3NoFiles() {
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class)))
                .thenReturn(ListObjectsV2Response.builder().contents(Collections.emptyList()).build());

        Map<String, String> result = service.debugS3(EQUIPMENT);

        assertThat(result.get("foundFiles")).isEqualTo("없음");
        assertThat(result).doesNotContainKey("schema");
    }

    @Test
    @DisplayName("#3 동시 호출에도 일관된 결과를 반환한다 (병렬 수행 안정성)")
    void concurrentCallsAreConsistent() throws Exception {
        stubS3Returns(new ParquetTestSupport.Row("TEMP", "C", 20.0));

        ExecutorService callers = Executors.newFixedThreadPool(8);
        try {
            List<CompletableFuture<MonthlySummaryResponseDTO>> futures = IntStream.range(0, 16)
                    .mapToObj(i -> CompletableFuture.supplyAsync(
                            () -> service.getMonthlySummary(EQUIPMENT), callers))
                    .collect(Collectors.toList());

            for (CompletableFuture<MonthlySummaryResponseDTO> f : futures) {
                MonthlySummaryResponseDTO r = f.get();
                assertThat(r.getSensors()).hasSize(1);
                assertThat(r.getSensors().get(0).getAvgValue()).isEqualTo(20.0);
                assertThat(r.getTotalDefects()).isEqualTo(7L);
                assertThat(r.getTotalAnomalies()).isEqualTo(3L);
            }
        } finally {
            callers.shutdownNow();
        }
    }
}
