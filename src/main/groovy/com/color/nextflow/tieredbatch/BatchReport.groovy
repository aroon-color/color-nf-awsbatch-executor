package com.color.nextflow.tieredbatch

import groovy.json.JsonOutput
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

/** Incremental snapshots are independent of retry state and contain no command or sample data. */
class BatchReport {
    final String launchId = UUID.randomUUID().toString()
    final Map<String, Map> attempts = new LinkedHashMap<>()
    private final Path directory
    private final String sessionId
    private long lastSnapshot

    BatchReport(Path directory, String sessionId) {
        this.directory = directory
        this.sessionId = sessionId
    }

    synchronized void record(Map attempt) {
        String key = "${attempt.jobId}:${attempt.backendAttempt}"
        attempts.put(key, (attempts.get(key) ?: [:]) + attempt)
        Files.writeString(directory.resolve('tiered-batch-attempts.jsonl'), JsonOutput.toJson(
            [schemaVersion: 1, launchId: launchId, sessionId: sessionId] + attempts.get(key)) + '\n',
            java.nio.file.StandardOpenOption.CREATE, java.nio.file.StandardOpenOption.APPEND)
        if (System.currentTimeMillis() - lastSnapshot >= 5000L) {
            writeSummary(false)
            lastSnapshot = System.currentTimeMillis()
        }
    }

    synchronized Map summary(boolean complete) {
        def rows = attempts.values().toList()
        def submitted = rows.groupBy { it.taskKey }.values()
        int spotRetries = 0
        int infrastructureRetries = 0
        int applicationRetries = 0
        for (def taskAttempts : submitted) {
            def ordered = taskAttempts.sort { a, b -> a.attempt <=> b.attempt ?: a.backendAttempt <=> b.backendAttempt }
            for (int index = 1; index < ordered.size(); index++) {
                def previous = ordered[index - 1]
                def current = ordered[index]
                if (previous.failure == 'INTERRUPTION' && current.market == 'SPOT') spotRetries++
                if (previous.failure == 'INFRASTRUCTURE') infrastructureRetries++
                if (previous.failure == 'ORDINARY' || (!previous.failure && previous.status == 'SUCCEEDED' && current.attempt > previous.attempt)) applicationRetries++
            }
        }
        def priced = rows.findAll { it.estimatedCostUsd != null }
        return [schemaVersion: 1, launchId: launchId, sessionId: sessionId, complete: complete,
            attempts: rows.size(), spotInterruptions: rows.count { it.failure == 'INTERRUPTION' },
            spotRetries: spotRetries, infrastructureRetries: infrastructureRetries, applicationRetries: applicationRetries,
            onDemandAttempts: rows.count { it.market == 'ON_DEMAND' },
            onDemandFallbacks: rows.count { it.fallback == true },
            spotInstances: rows.findAll { it.market == 'SPOT' && it.instanceId }.collect { it.instanceId }.unique().size(),
            onDemandInstances: rows.findAll { it.market == 'ON_DEMAND' && it.instanceId }.collect { it.instanceId }.unique().size(),
            unidentifiedAttempts: rows.count { !it.instanceId }, pricedAttempts: priced.size(),
            estimatedCostUsd: rows.isEmpty() ? 0G : priced ? priced.sum { it.estimatedCostUsd as BigDecimal } : null,
            runtimeSeconds: rows.groupBy { it.market }.collectEntries { market, values ->
                [(market): values.sum { it.startedAt && it.stoppedAt ? Math.max(0L, (it.stoppedAt as long) - (it.startedAt as long)) / 1000G : 0G }]
            },
            instances: rows.findAll { it.instanceId }.groupBy { it.instanceId }.collectEntries { id, values ->
                def costs = values.findAll { it.estimatedCostUsd != null }
                [(id): [market: values.first().market, instanceType: values.first().instanceType, attempts: values.size(),
                    estimatedCostUsd: costs ? costs.sum { it.estimatedCostUsd as BigDecimal } : null]]
            },
            costModel: 'Linux compute at observed hourly rates, allocated by mean(requested vCPU / host vCPU, requested memory / host memory); excludes idle capacity, leader, storage, network, discounts and billing minimums',
            tasks: rows.groupBy { it.taskKey }.collectEntries { key, values ->
                def costs = values.findAll { it.estimatedCostUsd != null }
                [(key): [attempts: values.size(), estimatedCostUsd: costs ? costs.sum { it.estimatedCostUsd as BigDecimal } : null]]
            }]
    }

    synchronized void writeSummary(boolean complete) {
        Path target = directory.resolve('tiered-batch-summary.json')
        Path temporary = directory.resolve('tiered-batch-summary.json.tmp')
        Files.writeString(temporary, JsonOutput.prettyPrint(JsonOutput.toJson(summary(complete))))
        Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING)
    }

    static BigDecimal estimate(Map row) {
        if (!row.startedAt || !row.stoppedAt || !row.hourlyRateUsd || !row.hostVcpus || !row.hostMemoryMiB || !row.vcpus || !row.memoryMiB)
            return null
        BigDecimal share = Math.min(1G, (((row.vcpus as BigDecimal) / row.hostVcpus) + ((row.memoryMiB as BigDecimal) / row.hostMemoryMiB)) / 2G)
        return ((row.stoppedAt as BigDecimal) - row.startedAt).max(0G) / 3600000G * (row.hourlyRateUsd as BigDecimal) * share
    }
}
