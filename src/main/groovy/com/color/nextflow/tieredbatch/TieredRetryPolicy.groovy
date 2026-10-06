package com.color.nextflow.tieredbatch

import groovy.transform.Immutable

class TieredRetryPolicy {
    enum Tier { SPOT, ON_DEMAND }

    @Immutable
    static class State {
        String spotQueue
        int interruptions
        int infrastructureFailures
        int ordinaryFailures
        boolean terminal
    }

    final TieredBatchConfig config

    TieredRetryPolicy(TieredBatchConfig config) { this.config = config }

    Tier tier(State state) {
        return (state.interruptions >= config.interruptionThreshold || state.infrastructureFailures >= config.infrastructureFailureThreshold) ? Tier.ON_DEMAND : Tier.SPOT
    }

    String queue(State state) {
        return tier(state) == Tier.ON_DEMAND ? config.queueMappings.get(state.spotQueue) : state.spotQueue
    }

    State fail(State state, Tier submittedTier, BatchFailureClassifier.Failure failure) {
        if (state.terminal)
            return state
        if (submittedTier == Tier.ON_DEMAND || failure == BatchFailureClassifier.Failure.UNRECOVERABLE)
            return new State(state.spotQueue, state.interruptions, state.infrastructureFailures, state.ordinaryFailures, true)
        if (failure == BatchFailureClassifier.Failure.INTERRUPTION)
            return new State(state.spotQueue, state.interruptions + 1, state.infrastructureFailures, state.ordinaryFailures, false)
        if (failure == BatchFailureClassifier.Failure.INFRASTRUCTURE)
            return new State(state.spotQueue, state.interruptions, state.infrastructureFailures + 1, state.ordinaryFailures, false)
        int failures = state.ordinaryFailures + 1
        return new State(state.spotQueue, state.interruptions, state.infrastructureFailures, failures, failures > config.ordinaryRetryAllowance)
    }
}
