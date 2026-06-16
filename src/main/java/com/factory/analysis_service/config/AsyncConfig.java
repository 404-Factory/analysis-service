package com.factory.analysis_service.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 30일치 S3 조회를 병렬화하기 위한 전용 스레드풀.
 *
 * <p>{@code parallelStream}의 공용 ForkJoinPool을 쓰지 않는 이유: S3 호출은 blocking I/O라
 * 공용 풀을 점유하면 애플리케이션의 다른 병렬 작업을 굶길 수 있다. I/O 바운드라 CPU 코어 수보다
 * 크게 잡아 동시 다운로드 처리량을 높인다.
 */
@Configuration
public class AsyncConfig {

    @Bean(name = "s3FetchExecutor", destroyMethod = "shutdown")
    public ExecutorService s3FetchExecutor() {
        ThreadFactory factory = new ThreadFactory() {
            private final AtomicInteger seq = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "s3-fetch-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
        return Executors.newFixedThreadPool(8, factory);
    }
}
