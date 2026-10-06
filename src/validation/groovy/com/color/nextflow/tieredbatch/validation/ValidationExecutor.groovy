package com.color.nextflow.tieredbatch.validation

import com.color.nextflow.tieredbatch.*
import nextflow.cloud.aws.batch.AwsBatchExecutor
import nextflow.cloud.aws.batch.AwsBatchProxy
import nextflow.processor.TaskRun
import nextflow.util.ServiceName

/** Test-only replacement of the external AWS boundary; never packaged with the production plugin. */
@ServiceName('tiered-awsbatch-validation')
class ValidationExecutor extends TieredBatchExecutor {
    private final FakeBatch api = new FakeBatch()

    @Override
    protected void validateWorkDir() { }

    @Override
    protected void createAwsClient() {
        super.createAwsClient()
        def clientField = AwsBatchExecutor.getDeclaredField('client')
        clientField.accessible = true
        def original = clientField.get(this)
        def submitterField = AwsBatchExecutor.getDeclaredField('submitter')
        submitterField.accessible = true
        clientField.set(this, new AwsBatchProxy(api.client(), submitterField.get(this)))
        session.onShutdown({ original.close() } as Runnable)
    }

    @Override
    String getJobOutputStream(String jobId) {
        return System.getenv('TIERED_VALIDATION_SCENARIO') == 'infra-stage-out' ? api.output(jobId) : null
    }

    @Override
    protected TieredBatchTaskHandler newTaskHandler(TaskRun task, String key, TieredRetryPolicy.Tier tier, String queue) {
        return new ValidationHandler(task, this, retryStates, key, tier, queue, api)
    }
}
