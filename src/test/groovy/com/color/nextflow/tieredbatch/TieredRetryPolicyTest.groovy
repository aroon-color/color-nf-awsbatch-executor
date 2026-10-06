package com.color.nextflow.tieredbatch

import nextflow.exception.ProcessUnrecoverableException
import spock.lang.Specification

class TieredRetryPolicyTest extends Specification {
    def config = new TieredBatchConfig([enabled: true, queueMappings: [spot: 'demand']])

    def 'three interruptions route only the affected logical task to a final attempt'() {
        given:
        def store = new TaskRetryStateStore(config)
        def first = store.prepare('process:1', 'spot', 1)
        def second = store.prepare('process:2', 'spot', 1)

        when:
        store.failed('process:1', 'job1', TieredRetryPolicy.Tier.SPOT, BatchFailureClassifier.Failure.INTERRUPTION)
        store.failed('process:1', 'job1', TieredRetryPolicy.Tier.SPOT, BatchFailureClassifier.Failure.INTERRUPTION)
        def retry = store.prepare('process:1', 'spot', 2)
        store.failed('process:1', 'job2', TieredRetryPolicy.Tier.SPOT, BatchFailureClassifier.Failure.INTERRUPTION)
        store.failed('process:1', 'job3', TieredRetryPolicy.Tier.SPOT, BatchFailureClassifier.Failure.INTERRUPTION)
        def finalAttempt = store.prepare('process:1', 'spot', 4)

        then:
        store.policy.queue(first) == 'spot'
        retry.interruptions == 1
        store.policy.queue(finalAttempt) == 'demand'
        store.policy.queue(second) == 'spot'

        when:
        store.failed('process:1', 'job4', TieredRetryPolicy.Tier.ON_DEMAND, BatchFailureClassifier.Failure.ORDINARY)
        store.prepare('process:1', 'spot', 5)

        then:
        thrown(ProcessUnrecoverableException)
    }

    def 'core output validation failures consume the ordinary allowance'() {
        given:
        def store = new TaskRetryStateStore(config)
        store.prepare('process:1', 'spot', 1)

        expect:
        store.prepare('process:1', 'spot', 3).ordinaryFailures == 2

        when:
        store.prepare('process:1', 'spot', 4)

        then:
        thrown(ProcessUnrecoverableException)
    }

    def 'configuration rejects invalid mappings and fractional retry allowances'() {
        when:
        new TieredBatchConfig(options)

        then:
        thrown(IllegalArgumentException)

        where:
        options << [[enabled: true], [queueMappings: [spot: 'spot']], [ordinaryRetryAllowance: 1.5], [interruptionThreshold: 0]]
    }
}
