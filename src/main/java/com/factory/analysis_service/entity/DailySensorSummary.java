package com.factory.analysis_service.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.domain.Persistable;

import java.io.Serializable;
import java.time.LocalDate;

/**
 * 설비/일자/센서타입별 일일 평균 요약값 캐시.
 *
 * <p>과거 일자의 S3 Parquet 집계 결과는 불변이므로 한 번 읽으면 영구 캐시할 수 있다.
 * 월간 요약은 항상 "1~30일 전"을 보기 때문에, 매 호출마다 29/30이 겹쳐 캐시 적중률이 높다.
 * defect/anomaly count는 가변이라 이 캐시에 넣지 않고 매번 실시간 조회한다.
 *
 * <p>복합키({@link IdClass})를 직접 할당하므로 Spring Data의 기본 isNew 판정이 항상 false가 되어
 * {@code save()}가 {@code merge}(UPDATE)로 동작한다. 캐시는 항상 신규 INSERT이므로
 * {@link Persistable}을 구현해 {@code persist}가 되도록 한다.
 */
@Entity
@Table(name = "daily_sensor_summary")
@IdClass(DailySensorSummary.Pk.class)
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailySensorSummary implements Persistable<DailySensorSummary.Pk> {

    @Id
    @Column(name = "equipment_id", nullable = false, length = 100)
    private String equipmentId;

    @Id
    @Column(name = "summary_date", nullable = false)
    private LocalDate summaryDate;

    @Id
    @Column(name = "sensor_type", nullable = false, length = 100)
    private String sensorType;

    @Column(name = "unit", length = 50)
    private String unit;

    @Column(name = "avg_value", nullable = false)
    private double avgValue;

    // 자연 기본값 false → 빌더/생성자로 만든 신규 엔티티는 isNew()=true (persist).
    // Hibernate가 조회로 적재하면 @PostLoad로 true가 되어 isNew()=false.
    @Transient
    private boolean loaded;

    @Override
    public Pk getId() {
        return new Pk(equipmentId, summaryDate, sensorType);
    }

    @Override
    public boolean isNew() {
        return !loaded;
    }

    @PostLoad
    void markLoaded() {
        this.loaded = true;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    @EqualsAndHashCode
    public static class Pk implements Serializable {
        private String equipmentId;
        private LocalDate summaryDate;
        private String sensorType;
    }
}
