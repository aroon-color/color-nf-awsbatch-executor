package com.color.nextflow.tieredbatch

import groovy.util.logging.Slf4j
import java.nio.file.Path
import java.time.Duration
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import nextflow.Session
import nextflow.cloud.aws.batch.AwsBatchTaskHandler
import nextflow.processor.TaskHandler
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import software.amazon.awssdk.services.batch.model.DescribeJobsRequest

@Slf4j
class BatchReportingObserver implements TraceObserver {
    private final Session session
    private final BatchReport report
    private final worker = Executors.newSingleThreadExecutor({ Runnable work ->
        Thread thread = new Thread(work, 'tiered-batch-reporting')
        thread.daemon = true
        return thread
    } as java.util.concurrent.ThreadFactory)
    private BatchMetadataResolver resolver
    private final Closure<Map> metadataProvider

    BatchReportingObserver(Session session, Closure<Map> metadataProvider = null) {
        this.metadataProvider = metadataProvider
        this.session = session
        report = new BatchReport(Path.of('.').toAbsolutePath(), session.uniqueId.toString())
        // Never mix resumed/cached results with the compute spent during this launch.
        java.nio.file.Files.deleteIfExists(Path.of('tiered-batch-attempts.jsonl'))
        java.nio.file.Files.deleteIfExists(Path.of('tiered-batch-summary.json'))
    }

    private void capture(TaskHandler handler, TraceRecord trace, boolean finished, boolean submitted = false) {
        if (!(handler instanceof AwsBatchTaskHandler) || !trace?.get('native_id')) return
        def task = handler.task
        def executor = task.processor.executor
        Map base = [jobId: trace.get('native_id').toString(), taskKey: "${task.processor.name}:${task.index}".toString(),
            attempt: task.config.attempt as int, backendAttempt: 1, queue: (trace.get('queue') ?: task.config.queue).toString(),
            region: executor.awsOptions.region, vcpus: task.config.cpus, memoryMiB: task.config.memory?.toMega(),
            status: finished ? 'COMPLETED' : submitted ? 'SUBMITTED' : 'RUNNING',
            failure: handler instanceof TieredBatchTaskHandler ? handler.reportedFailure?.toString() : null,
            fallback: handler instanceof TieredBatchTaskHandler && handler.reportedTier == TieredRetryPolicy.Tier.ON_DEMAND,
            market: handler instanceof TieredBatchTaskHandler ? handler.reportedTier.toString() :
                (session.config.navigate('tieredAwsBatch.queueMappings') as Map ?: [:]).containsKey(task.config.queue.toString()) ? 'SPOT' : 'UNKNOWN']
        worker.submit {
            try {
                report.record(base)
                if (submitted) return
                def limits = AwsRequestOverrideConfiguration.builder().apiCallTimeout(Duration.ofSeconds(5)).apiCallAttemptTimeout(Duration.ofSeconds(3)).build()
                def job = executor.client.describeJobs(DescribeJobsRequest.builder().jobs(base.jobId as String).overrideConfiguration(limits).build()).jobs().find()
                if (!job) return
                if (!metadataProvider && !resolver && (job.container()?.containerInstanceArn() || job.attempts().any { it.container()?.containerInstanceArn() })) resolver = new BatchMetadataResolver(session.config.aws as Map ?: [:])
                def backend = job.attempts()
                def runs = backend ? new ArrayList(backend) : [null]
                if (backend && job.statusAsString() == 'RUNNING' && job.container()?.taskArn() &&
                    !backend.any { it.container()?.taskArn() == job.container().taskArn() }) runs.add(null)
                runs.eachWithIndex { run, index ->
                    Map row = base + [backendAttempt: index + 1, status: job.statusAsString(),
                        startedAt: run?.startedAt() ?: job.startedAt(), stoppedAt: run?.stoppedAt() ?: job.stoppedAt(),
                        ecsTaskArn: run?.container()?.taskArn() ?: job.container()?.taskArn(),
                        containerInstanceArn: run?.container()?.containerInstanceArn() ?: job.container()?.containerInstanceArn()]
                    try {
                        if (metadataProvider) row.putAll(metadataProvider.call(row))
                        else if (resolver) row.putAll(resolver.resolve(row))
                    }
                    catch (RuntimeException error) { row.metadataError = error.class.simpleName }
                    if (run?.statusReason()?.startsWith('Host EC2') && row.market == 'SPOT')
                        row.failure = 'INTERRUPTION'
                    else if (!row.failure && (run?.container()?.exitCode() || row.status == 'FAILED'))
                        row.failure = BatchFailureClassifier.classify(job, row.market == 'SPOT').toString()
                    def hostReason = run?.statusReason() ?: job.statusReason() ?: ''
                    def identity = hostReason =~ /Host EC2 \(instance (i-[a-z0-9]+)\)/
                    if (!row.instanceId && identity.find()) row.instanceId = identity.group(1)
                    row.estimatedCostUsd = BatchReport.estimate(row)
                    report.record(row)
                }
            }
            catch (Exception error) { log.warn "AWS Batch reporting incomplete: ${error.class.simpleName}" }
        }
    }

    @Override void onProcessSubmit(TaskHandler handler, TraceRecord trace) { capture(handler, trace, false, true) }
    @Override void onProcessStart(TaskHandler handler, TraceRecord trace) { capture(handler, trace, false) }
    @Override void onProcessComplete(TaskHandler handler, TraceRecord trace) { capture(handler, trace, true) }
    @Override void onFlowComplete() {
        worker.shutdown()
        boolean drained = false
        try { drained = worker.awaitTermination(30, TimeUnit.SECONDS) }
        catch (InterruptedException error) { Thread.currentThread().interrupt() }
        if (!drained) worker.shutdownNow()
        try { report.writeSummary(drained) }
        catch (Exception error) { log.warn "Unable to finalize AWS Batch reporting: ${error.class.simpleName}" }
        if (drained) {
            try { resolver?.close() }
            catch (RuntimeException error) { log.debug 'Unable to close reporting clients', error }
        }
    }
}
