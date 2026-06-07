package com.factory.analysis_service.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SensorSummaryDTO {
    private String sensorType;
    private String unit;
    private double avgValue;
}
