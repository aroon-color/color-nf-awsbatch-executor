package com.color.nextflow.tieredbatch

import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

@ScopeName('tieredAwsBatch')
@Description('AWS Batch retries on Spot with one final on-demand execution.')
class TieredBatchConfig implements ConfigScope {
    @ConfigOption
    @Description('Enable the tiered executor. Default: false.')
    final boolean enabled

    @ConfigOption
    @Description('Host interruptions per task before on-demand fallback. Default: 3.')
    final int interruptionThreshold

    @ConfigOption
    @Description('Ordinary failure retries before fallback. Default: 2.')
    final int ordinaryRetryAllowance

    @ConfigOption(types = [Map])
    @Description('Mapping of Spot-only queues to on-demand-only queues.')
    final Map<String, String> queueMappings

    TieredBatchConfig() { this([:]) }

    TieredBatchConfig(Map options) {
        enabled = options.enabled == true
        interruptionThreshold = integerOption(options, 'interruptionThreshold', 3, 1)
        ordinaryRetryAllowance = integerOption(options, 'ordinaryRetryAllowance', 2, 0)
        if (options.queueMappings != null && !(options.queueMappings instanceof Map))
            throw new IllegalArgumentException('tieredAwsBatch.queueMappings must be a map')
        Map<String, String> mappings = [:]
        (options.queueMappings ?: [:]).each { spot, demand ->
            if (!(spot instanceof String) || !(demand instanceof String) || !spot.trim() || !demand.trim() || spot == demand)
                throw new IllegalArgumentException('Queue mappings require distinct, nonempty Spot and on-demand queue names or ARNs')
            mappings.put(spot, demand)
        }
        if (enabled && mappings.isEmpty())
            throw new IllegalArgumentException('tieredAwsBatch.queueMappings is required when enabled')
        queueMappings = Collections.unmodifiableMap(mappings)
    }

    private static int integerOption(Map options, String name, int fallback, int minimum) {
        def value = options.containsKey(name) ? options.get(name) : fallback
        if (!(value instanceof Integer) || value < minimum || value > 100)
            throw new IllegalArgumentException("tieredAwsBatch.${name} must be an integer between ${minimum} and 100")
        return value
    }
}
