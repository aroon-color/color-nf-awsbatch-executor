package com.color.nextflow.tieredbatch.validation

import com.color.nextflow.tieredbatch.*
import nextflow.cloud.aws.batch.AwsBatchExecutor
import nextflow.cloud.aws.batch.AwsBatchProxy
import nextflow.processor.TaskRun
import nextflow.processor.TaskHandler
import nextflow.cloud.aws.batch.AwsBatchTaskHandler
import software.amazon.awssdk.services.batch.model.SubmitJobRequest
import nextflow.util.ServiceName

/** Test-only replacement of the external AWS boundary; never packaged with the production plugin. */
@ServiceName('awsbatch-validation')
class ValidationAwsExecutor extends AwsBatchExecutor {
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
    TaskHandler createTaskHandler(TaskRun task) {
        return new FixtureHandler(task, this, api)
    }

    private static class FixtureHandler extends AwsBatchTaskHandler {
        private final FakeBatch api
        FixtureHandler(TaskRun task, AwsBatchExecutor executor, FakeBatch api) {
            super(task, executor)
            this.api = api
        }
        @Override protected SubmitJobRequest newSubmitRequest(TaskRun task) {
            def request = super.newSubmitRequest(task)
            api.bind(request.jobName(), task)
            return request
        }
    }
}
