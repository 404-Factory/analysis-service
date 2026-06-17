package com.factory.analysis_service.service;

import com.factory.analysis_service.config.AwsS3Properties;
import com.factory.analysis_service.dto.MonthlySummaryResponseDTO;
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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 성능 테스트: S3 호출에 인위적 지연(각 40ms)을 주입하여 30일 조회가 '직렬'이 아니라 '병렬'로
 * 수행되는지 확인한다.
 *
 * <ul>
 *   <li>직렬 하한: 30일 × (list 40ms + get 40ms) = 약 2400ms</li>
 *   <li>병렬(풀 8): ceil(30/8)=4 웨이브 × 80ms ≈ 320ms</li>
 * </ul>
 * 임계값 1500ms로, 직렬이라면 절대 통과할 수 없고 병렬이면 충분히 통과하도록 둔다.
 */
@DisplayName("S3SummaryService 성능 - 병렬 조회 검증")
class S3SummaryServicePerformanceTest {

    private static final long IO_LATENCY_MS = 40;
    private static final long SERIAL_LOWER_BOUND_MS = 30 * (IO_LATENCY_MS * 2); // 2400ms
    private static final long PARALLEL_THRESHOLD_MS = 1500;

    private S3Client s3Client;
    private ExecutorService executor;
    private S3SummaryService service;

    @BeforeEach
    void setUp() {
        s3Client = mock(S3Client.class);
        DailySensorSummaryRepository cacheRepository = mock(DailySensorSummaryRepository.class);
        when(cacheRepository.findByEquipmentIdAndSummaryDateBetween(any(), any(), any()))
                .thenReturn(Collections.emptyList());
        when(cacheRepository.saveAll(any())).thenAnswer(inv -> inv.getArgument(0));

        executor = Executors.newFixedThreadPool(8);

        AwsS3Properties props = new AwsS3Properties();
        props.getS3().setBucket("test-bucket");
        props.setRegion("ap-northeast-2");

        byte[] parquet = ParquetTestSupport.parquetBytes(
                List.of(new ParquetTestSupport.Row("TEMP", "C", 20.0)));

        ListObjectsV2Response listResp = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("summary-data/part-0.parquet").build())
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenAnswer(inv -> {
            sleep(IO_LATENCY_MS);
            return listResp;
        });
        ResponseBytes<GetObjectResponse> objResp =
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), parquet);
        when(s3Client.getObjectAsBytes(any(GetObjectRequest.class))).thenAnswer(inv -> {
            sleep(IO_LATENCY_MS);
            return objResp;
        });

        service = new S3SummaryService(
                s3Client, props, StubWebClients.counting(0, 0), cacheRepository, executor);
        ReflectionTestUtils.setField(service, "managementServiceUrl", "http://mgmt");
        ReflectionTestUtils.setField(service, "anomalyServiceUrl", "http://anomaly");
    }

    @AfterEach
    void tearDown() {
        executor.shutdownNow();
    }

    @Test
    @DisplayName("30일 S3 조회가 병렬로 수행되어 직렬 하한보다 현저히 빠르다")
    void monthlyFetchRunsInParallel() {
        // 워밍업 1회 (JIT/클래스 로딩 영향 제거)
        service.getMonthlySummary("EQP-01");

        long start = System.nanoTime();
        MonthlySummaryResponseDTO result = service.getMonthlySummary("EQP-01");
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;

        System.out.printf("[perf] 30일 월간요약 소요=%dms (직렬 하한=%dms, 임계=%dms)%n",
                elapsedMs, SERIAL_LOWER_BOUND_MS, PARALLEL_THRESHOLD_MS);

        assertThat(result.getSensors()).hasSize(1);
        assertThat(elapsedMs)
                .as("병렬이면 %dms 미만, 직렬이면 ~%dms", PARALLEL_THRESHOLD_MS, SERIAL_LOWER_BOUND_MS)
                .isLessThan(PARALLEL_THRESHOLD_MS);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
