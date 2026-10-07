package com.color.nextflow.tieredbatch

import nextflow.Session
import nextflow.exception.ProcessUnrecoverableException

class TaskRetryStateStore {
    private static final Map<Session, TaskRetryStateStore> SESSIONS = new WeakHashMap<>()
    private final Map<String, TieredRetryPolicy.State> states = [:]
    private final Set<String> recordedFailures = new HashSet<>()
    private final Map<String, String> submissions = [:]
    final TieredRetryPolicy policy

    TaskRetryStateStore(TieredBatchConfig config) { policy = new TieredRetryPolicy(config) }

    static synchronized TaskRetryStateStore forSession(Session session, TieredBatchConfig config) {
        if (!SESSIONS.containsKey(session)) {
            SESSIONS.put(session, new TaskRetryStateStore(config))
            session.onShutdown({ release(session) } as Runnable)
        }
        return SESSIONS.get(session)
    }

    private static synchronized void release(Session session) { SESSIONS.remove(session) }

    synchronized TieredRetryPolicy.State prepare(String taskKey, String originalQueue, int attempt) {
        if (!states.containsKey(taskKey)) {
            if (!policy.config.queueMappings.containsKey(originalQueue))
                throw new ProcessUnrecoverableException("No on-demand mapping for Spot queue '${originalQueue}'")
            states.put(taskKey, new TieredRetryPolicy.State(originalQueue, 0, 0, 0, false))
        }
        def state = states.get(taskKey)
        // Core validation errors (e.g. missing outputs) also consume ordinary retries.
        int ordinaryFailures = Math.max(state.ordinaryFailures, attempt - 1 - state.interruptions - state.infrastructureFailures)
        if (state.terminal || ordinaryFailures > policy.config.ordinaryRetryAllowance)
            throw new ProcessUnrecoverableException("Tiered retry allowance exhausted for ${taskKey}")
        state = new TieredRetryPolicy.State(state.spotQueue, state.interruptions, state.infrastructureFailures, ordinaryFailures, false)
        states.put(taskKey, state)
        return state
    }

    synchronized void submitted(String taskKey, int attempt, String jobId) {
        String key = "${taskKey}:${attempt}"
        if (submissions.containsKey(key) && submissions.get(key) != jobId)
            throw new ProcessUnrecoverableException("Duplicate AWS Batch submission for ${key}; inspect jobs before resuming")
        submissions.put(key, jobId)
    }

    synchronized TieredRetryPolicy.State failed(String taskKey, String jobId, TieredRetryPolicy.Tier tier, BatchFailureClassifier.Failure failure) {
        String key = "${taskKey}:${jobId}"
        if (recordedFailures.add(key))
            states.put(taskKey, policy.fail(states.get(taskKey), tier, failure))
        return states.get(taskKey)
    }
}
