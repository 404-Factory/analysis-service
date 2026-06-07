package com.factory.analysis_service.controller;

import com.factory.common.core.dto.ApiResponse;
import com.factory.analysis_service.dto.MonthlySummaryResponseDTO;
import com.factory.analysis_service.service.S3SummaryService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.Map;

@RestController
@RequestMapping("/api/summary")
@RequiredArgsConstructor
public class SummaryController {

    private final S3SummaryService s3SummaryService;

    @GetMapping("/{equipmentName}/monthly")
    public ApiResponse<MonthlySummaryResponseDTO> getMonthlySummary(
            @PathVariable String equipmentName) {
        return ApiResponse.ofSuccess(s3SummaryService.getMonthlySummary(equipmentName));
    }

    @GetMapping("/debug/{equipmentName}")
    public Map<String, String> debug(@PathVariable String equipmentName) {
        return s3SummaryService.debugS3(equipmentName);
    }
}
