package com.color.nextflow.tieredbatch

import groovy.json.JsonSlurper
import java.nio.file.Files
import spock.lang.Specification
import spock.lang.TempDir

class BatchReportTest extends Specification {
    @TempDir java.nio.file.Path directory

    def 'retry counters distinguish interruptions, spot resubmissions and distinct hosts'() {
        given:
        def report = new BatchReport(directory, 'session')
        for (int attempt = 1; attempt <= 4; attempt++) {
            report.record([taskKey: 'task:1', jobId: "job${attempt}", backendAttempt: 1, attempt: attempt,
                market: attempt == 4 ? 'ON_DEMAND' : 'SPOT', instanceId: attempt == 4 ? 'i-demand' : 'i-spot',
                failure: attempt == 4 ? null : 'INTERRUPTION', fallback: attempt == 4,
                estimatedCostUsd: 0.1G])
        }
        // Polling updates the same attempt, rather than charging it twice.
        report.record([jobId: 'job4', backendAttempt: 1, status: 'SUCCEEDED'])

        when:
        def summary = report.summary(true)

        then:
        summary.attempts == 4
        summary.spotInterruptions == 3
        summary.spotRetries == 2
        summary.onDemandFallbacks == 1
        summary.onDemandAttempts == 1
        summary.spotInstances == 1
        summary.onDemandInstances == 1
        summary.estimatedCostUsd == 0.4G
        summary.tasks['task:1'].estimatedCostUsd == 0.4G
        new JsonSlurper().parse(directory.resolve('tiered-batch-summary.json').toFile()).complete == false
        Files.readAllLines(directory.resolve('tiered-batch-attempts.jsonl')).size() == 5
    }

    def 'allocated cost accounts for memory and never charges a small task the whole instance'() {
        expect:
        BatchReport.estimate([startedAt: 1000L, stoppedAt: 3601000L, hourlyRateUsd: 1G,
            vcpus: cpus, memoryMiB: memory, hostVcpus: 8, hostMemoryMiB: 32768]) == expected

        where:
        cpus | memory | expected
        1    | 1024   | 0.078125G
        1    | 16384  | 0.3125G
        8    | 32768  | 1G
    }

    def 'complementary CPU and memory reservations sum to one shared host cost'() {
        given:
        def common = [startedAt: 1000L, stoppedAt: 3601000L, hourlyRateUsd: 1G, hostVcpus: 8, hostMemoryMiB: 32768]
        expect:
        BatchReport.estimate(common + [vcpus: 7, memoryMiB: 4096]) +
            BatchReport.estimate(common + [vcpus: 1, memoryMiB: 28672]) == 1G
    }

    def 'unknown rates and unfinished jobs remain unpriced'() {
        given:
        def report = new BatchReport(directory, 'session')
        report.record([jobId: 'j1', backendAttempt: 1, taskKey: 'task:1', attempt: 1, market: 'UNKNOWN'])

        expect:
        report.summary(false).estimatedCostUsd == null
        report.summary(false).pricedAttempts == 0
        report.summary(false).unidentifiedAttempts == 1
        BatchReport.estimate([startedAt: 1000L]) == null
        new BatchReport(directory, 'resumed-session').summary(true).attempts == 0
    }
}
