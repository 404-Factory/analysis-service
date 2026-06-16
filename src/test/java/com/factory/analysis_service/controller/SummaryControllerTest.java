package com.factory.analysis_service.controller;

import com.factory.analysis_service.dto.MonthlySummaryResponseDTO;
import com.factory.analysis_service.dto.SensorSummaryDTO;
import com.factory.analysis_service.service.S3SummaryService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SummaryController.class)
@ActiveProfiles("test")
@DisplayName("SummaryController")
class SummaryControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private S3SummaryService s3SummaryService;

    @Test
    @DisplayName("월간 요약을 ApiResponse로 감싸 반환한다")
    void getMonthlySummary() throws Exception {
        MonthlySummaryResponseDTO dto = MonthlySummaryResponseDTO.builder()
                .totalDefects(7L)
                .totalAnomalies(3L)
                .sensors(List.of(SensorSummaryDTO.builder()
                        .sensorType("TEMP").unit("C").avgValue(20.5).build()))
                .build();
        when(s3SummaryService.getMonthlySummary(eq("EQP-01"))).thenReturn(dto);

        mockMvc.perform(get("/api/summary/{equipmentName}/monthly", "EQP-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").value(true))
                .andExpect(jsonPath("$.data.totalDefects").value(7))
                .andExpect(jsonPath("$.data.totalAnomalies").value(3))
                .andExpect(jsonPath("$.data.sensors[0].sensorType").value("TEMP"))
                .andExpect(jsonPath("$.data.sensors[0].avgValue").value(20.5));
    }

    @Test
    @DisplayName("debug 엔드포인트는 S3 진단 맵을 반환한다")
    void debug() throws Exception {
        when(s3SummaryService.debugS3(eq("EQP-01")))
                .thenReturn(Map.of("bucket", "test-bucket", "foundFiles", "x.parquet"));

        mockMvc.perform(get("/api/summary/debug/{equipmentName}", "EQP-01"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.bucket").value("test-bucket"))
                .andExpect(jsonPath("$.foundFiles").value("x.parquet"));
    }
}
