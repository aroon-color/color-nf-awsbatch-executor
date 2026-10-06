package com.color.nextflow.tieredbatch

import software.amazon.awssdk.core.SdkPlugin
import software.amazon.awssdk.core.SdkServiceClientConfiguration
import software.amazon.awssdk.retries.StandardRetryStrategy

/** SubmitJob has no idempotency token: a lost response must not create another job. */
class SingleSubmissionPlugin implements SdkPlugin {
    @Override
    void configureClient(SdkServiceClientConfiguration.Builder builder) {
        builder.overrideConfiguration(builder.overrideConfiguration().toBuilder()
            .retryStrategy(StandardRetryStrategy.builder().maxAttempts(1).build()).build())
    }
}
