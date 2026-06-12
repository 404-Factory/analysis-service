package com.factory.analysis_service.support;

import org.springframework.http.HttpStatus;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

/**
 * 실제 네트워크 없이 management/anomaly count 응답을 흉내내는 WebClient 스텁.
 * URL 경로로 라우팅하여 ApiResponse&lt;Long&gt; JSON을 돌려준다.
 */
public final class StubWebClients {

    private StubWebClients() {
    }

    /** 두 서비스가 각각 defects/anomalies count를 정상 반환하는 WebClient. */
    public static WebClient counting(long defects, long anomalies) {
        return fromExchange(request -> {
            String url = request.url().toString();
            if (url.contains("/defects/count")) {
                return Mono.just(json("{\"success\":true,\"status\":200,\"data\":" + defects + "}"));
            }
            if (url.contains("/anomalies/count")) {
                return Mono.just(json("{\"success\":true,\"status\":200,\"data\":" + anomalies + "}"));
            }
            return Mono.just(ClientResponse.create(HttpStatus.NOT_FOUND).build());
        });
    }

    /** management는 5xx로 실패, anomaly는 data 필드가 없는(null) 응답 — 둘 다 0으로 폴백되어야 함. */
    public static WebClient failingManagementAndNullAnomaly() {
        return fromExchange(request -> {
            String url = request.url().toString();
            if (url.contains("/defects/count")) {
                return Mono.just(ClientResponse.create(HttpStatus.INTERNAL_SERVER_ERROR).build());
            }
            if (url.contains("/anomalies/count")) {
                return Mono.just(json("{\"success\":true,\"status\":200}")); // data 없음 → null
            }
            return Mono.just(ClientResponse.create(HttpStatus.NOT_FOUND).build());
        });
    }

    private static WebClient fromExchange(ExchangeFunction exchange) {
        return WebClient.builder().exchangeFunction(exchange).build();
    }

    private static ClientResponse json(String body) {
        return ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body(body)
                .build();
    }
}
