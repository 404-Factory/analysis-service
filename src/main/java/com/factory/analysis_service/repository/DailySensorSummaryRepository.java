package com.factory.analysis_service.repository;

import com.factory.analysis_service.entity.DailySensorSummary;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.List;

public interface DailySensorSummaryRepository
        extends JpaRepository<DailySensorSummary, DailySensorSummary.Pk> {

    /**
     * 한 번의 쿼리로 설비의 기간 내 캐시된 모든 일자/센서 요약을 가져온다.
     * (30일치를 일자별로 30번 조회하지 않기 위함)
     */
    List<DailySensorSummary> findByEquipmentIdAndSummaryDateBetween(
            String equipmentId, LocalDate from, LocalDate to);
}
