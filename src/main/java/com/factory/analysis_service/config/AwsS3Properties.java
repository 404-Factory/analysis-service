package com.factory.analysis_service.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "cloud.aws")
public class AwsS3Properties {

    private S3 s3 = new S3();
    private String region;
    private Credentials credentials = new Credentials();

    @Getter
    @Setter
    public static class S3 {
        private String bucket;
    }

    @Getter
    @Setter
    public static class Credentials {
        private String accessKey = "";
        private String secretKey = "";
    }
}
