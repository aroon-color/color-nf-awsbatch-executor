package com.color.nextflow.tieredbatch.validation

import com.color.nextflow.tieredbatch.*
import nextflow.processor.TaskRun
import software.amazon.awssdk.services.batch.model.SubmitJobRequest

class ValidationHandler extends TieredBatchTaskHandler {
    private final FakeBatch api

    ValidationHandler(TaskRun task, TieredBatchExecutor executor, TaskRetryStateStore store, String key, TieredRetryPolicy.Tier tier, String queue, FakeBatch api) {
        super(task, executor, store, key, tier, queue)
        this.api = api
    }

    @Override
    protected SubmitJobRequest newSubmitRequest(TaskRun task) {
        def request = super.newSubmitRequest(task)
        api.bind(request.jobName(), task)
        return request
    }
}
