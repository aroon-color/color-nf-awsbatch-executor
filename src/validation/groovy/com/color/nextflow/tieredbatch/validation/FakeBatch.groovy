package com.color.nextflow.tieredbatch.validation

import java.lang.reflect.Proxy
import nextflow.processor.TaskRun
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model.*

class FakeBatch {
    private final Map<String, TaskRun> tasks = [:]
    private final Map<String, JobDetail> jobs = [:]
    private final Map<String, String> outputs = [:]
    private int sequence

    synchronized void bind(String jobName, TaskRun task) { tasks.put(jobName, task) }

    synchronized String output(String jobId) { return outputs.get(jobId) }

    BatchClient client() {
        return Proxy.newProxyInstance(BatchClient.classLoader, [BatchClient] as Class[], { proxy, method, arguments ->
            switch (method.name) {
                case 'serviceName': return 'batch'
                case 'close': return null
                case 'toString': return 'FakeBatch'
                case 'describeJobQueues':
                    return DescribeJobQueuesResponse.builder().jobQueues(arguments[0].jobQueues().collect { queue ->
                        JobQueueDetail.builder().jobQueueName(queue).jobQueueArn("arn:aws:batch:us-east-1:000000000000:job-queue/${queue}")
                            .state('ENABLED').status('VALID').computeEnvironmentOrder(ComputeEnvironmentOrder.builder().order(1).computeEnvironment(queue).build()).build()
                    }).build()
                case 'describeComputeEnvironments':
                    return DescribeComputeEnvironmentsResponse.builder().computeEnvironments(arguments[0].computeEnvironments().collect { environment ->
                        ComputeEnvironmentDetail.builder().computeEnvironmentName(environment).state('ENABLED').status('VALID')
                            .computeResources(ComputeResource.builder().type(environment.contains('spot') ? 'SPOT' : 'EC2').maxvCpus(32).build()).build()
                    }).build()
                case 'describeJobDefinitions': return DescribeJobDefinitionsResponse.builder().jobDefinitions([]).build()
                case 'registerJobDefinition':
                    return RegisterJobDefinitionResponse.builder().jobDefinitionName(arguments[0].jobDefinitionName()).revision(1)
                        .jobDefinitionArn("arn:aws:batch:us-east-1:000000000000:job-definition/${arguments[0].jobDefinitionName()}:1").build()
                case 'submitJob': return submit(arguments[0])
                case 'describeJobs': return DescribeJobsResponse.builder().jobs(arguments[0].jobs().collect { jobs.get(it) }.findAll()).build()
                case 'terminateJob': return TerminateJobResponse.builder().build()
                default: throw new UnsupportedOperationException("Unexpected AWS call: ${method.name}")
            }
        } as java.lang.reflect.InvocationHandler) as BatchClient
    }

    private synchronized SubmitJobResponse submit(SubmitJobRequest request) {
        def task = tasks.get(request.jobName())
        String jobId = "fixture-${++sequence}"
        String scenario = System.getenv('TIERED_VALIDATION_SCENARIO') ?: 'success'
        int attempt = task.config.attempt as int
        boolean taskA = task.processor.name.endsWith('taskA')
        boolean onDemand = request.jobQueue().contains('demand')
        int interruptionEnd = scenario == 'ordinary-first' ? 4 : 3
        boolean ordinary = taskA && ((scenario == 'ordinary-first' && attempt == 1) || scenario == 'ordinary-only')
        boolean interrupted = taskA && !ordinary && attempt <= interruptionEnd
        if (scenario == 'isolation' && !taskA)
            interrupted = attempt == 1
        if (scenario == 'final-host' && onDemand)
            interrupted = true
        boolean infrastructure = taskA && scenario.startsWith('infra-') && !onDemand
        if (scenario.startsWith('infra-')) {
            interrupted = false
            ordinary = scenario == 'infra-permanent' && taskA
            infrastructure = infrastructure && !ordinary
        }
        if (scenario == 'infra-mixed' && taskA && !onDemand) {
            ordinary = attempt == 2
            interrupted = attempt == 3
            infrastructure = !ordinary && !interrupted
        }
        if (scenario == 'infra-final' && taskA && onDemand)
            infrastructure = true
        int exitCode = infrastructure ? 1 : interrupted ? 143 : ordinary || (scenario == 'final-failure' && onDemand) ? 1 : 0
        String reason = interrupted ? 'Host EC2 (instance i-fixture) terminated.' : exitCode ? 'Essential container in task exited' : ''
        if (infrastructure || (scenario == 'infra-permanent' && taskA)) {
            String message = scenario == 'infra-stage-out' ?
                'upload failed: ./result.txt to s3://fixture/result.txt Read timeout on endpoint URL' :
                scenario == 'infra-permanent' ? 'download failed: s3://fixture/input to ./input AccessDenied: Access Denied' :
                "download failed: s3://fixture/reference.fa to ./reference.fa ConnectionResetError(104, 'Connection reset by peer')"
            outputs.put(jobId, message)
            if (scenario == 'infra-stage-out')
                task.workDir.resolve('.exitcode').toFile().text = '0'
            task.workDir.resolve('.command.err').toFile().text = ('worker output\n' * 6000) + message
            task.workDir.resolve('.command.log').toFile().text = ('worker output\n' * 6000) + message
        }
        if (!exitCode && !(scenario == 'missing-output' && onDemand)) {
            def process = new ProcessBuilder('bash', '.command.sh').directory(task.workDir.toFile())
                .redirectOutput(task.workDir.resolve('.command.out').toFile()).redirectError(task.workDir.resolve('.command.err').toFile()).start()
            exitCode = process.waitFor()
        }
        if (!exitCode)
            task.workDir.resolve('.exitcode').toFile().text = '0'
        def container = ContainerDetail.builder().exitCode(exitCode).build()
        jobs.put(jobId, JobDetail.builder().jobId(jobId).jobName(request.jobName()).jobQueue(request.jobQueue())
            .status(exitCode ? 'FAILED' : 'SUCCEEDED').statusReason(reason).container(container)
            .attempts(AttemptDetail.builder().statusReason(reason).container(AttemptContainerDetail.builder().exitCode(exitCode).build()).build()).build())
        def resources = request.containerOverrides().resourceRequirements().collect { "${it.typeAsString()}=${it.value()}" }.sort().join(',')
        new File(System.getenv('TIERED_VALIDATION_EVENTS')).append("${task.processor.name}\t${request.jobQueue()}\t${jobId}\t${request.retryStrategy().attempts()}\t${request.tags().get('job-queue')}\t${resources}\n")
        if (scenario == 'lost-response' && taskA)
            throw SdkClientException.create('Fixture accepted job but lost the submission response')
        return SubmitJobResponse.builder().jobId(jobId).jobName(request.jobName()).build()
    }
}
