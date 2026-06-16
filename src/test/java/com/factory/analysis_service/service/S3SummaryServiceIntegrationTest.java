package com.factory.analysis_service.service;

import com.factory.analysis_service.dto.MonthlySummaryResponseDTO;
import com.factory.analysis_service.entity.DailySensorSummary;
import com.factory.analysis_service.repository.DailySensorSummaryRepository;
import com.factory.analysis_service.support.ParquetTestSupport;
import com.factory.analysis_service.support.StubWebClients;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.web.reactive.function.client.WebClient;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("test")
@DisplayName("S3SummaryService 통합 테스트 (H2 캐시)")
class S3SummaryServiceIntegrationTest {

    @TestConfiguration
    static class StubConfig {
        @Bean
        @Primary
        WebClient stubWebClient() {
            return StubWebClients.counting(11, 5);
        }
    }

    @MockitoBean
    private S3Client s3Client;

    @Autowired
    private S3SummaryService service;

    @Autowired
    private DailySensorSummaryRepository repository;

    @BeforeEach
    void setUp() {
        repository.deleteAll();

        ListObjectsV2Response listResp = ListObjectsV2Response.builder()
                .contents(S3Object.builder().key("summary-data/part-0.parquet").build())
                .build();
        when(s3Client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(listResp);

        byte[] bytes = ParquetTestSupport.parquetBytes(List.of(
                new ParquetTestSupport.Row("TEMP", "C", 20.0),
                new ParquetTestSupport.Row("PRESSURE", "kPa", 100.0)));
        ResponseBytes<GetObjectResponse> resp =
                ResponseBytes.fromByteArray(GetObjectResponse.builder().build(), bytes);
        when(s3Client.getObjectAsBytes(any(GetObjectRequest.class))).thenReturn(resp);
    }

    @AfterEach
    void tearDown() {
        repository.deleteAllInBatch();
    }

    @Test
    @DisplayName("#1#2#4 S3 데이터 조회 + 외부 count 통신 결과를 반환하고, 일자별 요약을 H2에 저장한다")
    void fetchesAggregatesAndPersistsCache() {
        MonthlySummaryResponseDTO result = service.getMonthlySummary("EQP-01");

        // #1 센서 집계
        assertThat(result.getSensors()).hasSize(2);
        // #2 외부 서비스 count
        assertThat(result.getTotalDefects()).isEqualTo(11L);
        assertThat(result.getTotalAnomalies()).isEqualTo(5L);

        // #4 MariaDB(H2)에 30일 × 2센서 = 60행 저장됨
        List<DailySensorSummary> persisted = repository.findAll();
        assertThat(persisted).hasSize(60);
        assertThat(persisted).allSatisfy(row ->
                assertThat(row.getEquipmentId()).isEqualTo("EQP-01"));
    }

    @Test
    @DisplayName("#4 두 번째 호출은 H2 캐시를 사용하여 S3를 다시 조회하지 않는다")
    void secondCallUsesCacheFromDb() {
        // 1차 호출 — S3에서 읽어 캐시 적재
        service.getMonthlySummary("EQP-01");
        assertThat(repository.count()).isEqualTo(60);

        clearInvocations(s3Client);

        // 2차 호출 — 같은 날짜 범위라 전부 캐시 적중
        MonthlySummaryResponseDTO result = service.getMonthlySummary("EQP-01");

        assertThat(result.getSensors()).hasSize(2);
        verify(s3Client, never()).listObjectsV2(any(ListObjectsV2Request.class));
        verify(s3Client, never()).getObjectAsBytes(any(GetObjectRequest.class));
    }
}
