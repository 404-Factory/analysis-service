package com.factory.analysis_service.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.S3Client;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("S3Config - 자격증명 분기")
class S3ConfigTest {

    private AwsS3Properties props(String accessKey, String secretKey) {
        AwsS3Properties p = new AwsS3Properties();
        p.setRegion("ap-northeast-2");
        p.getS3().setBucket("test-bucket");
        p.getCredentials().setAccessKey(accessKey);
        p.getCredentials().setSecretKey(secretKey);
        return p;
    }

    @Test
    @DisplayName("access/secret 키가 있으면 StaticCredentialsProvider로 S3Client를 만든다")
    void staticCredentials() {
        S3Client client = new S3Config(props("AKIA-TEST", "secret")).s3Client();
        assertThat(client).isNotNull();
        client.close();
    }

    @Test
    @DisplayName("키가 비어있으면 DefaultCredentialsProvider(IAM)로 S3Client를 만든다")
    void defaultCredentials() {
        S3Client client = new S3Config(props("", "")).s3Client();
        assertThat(client).isNotNull();
        client.close();
    }
}
