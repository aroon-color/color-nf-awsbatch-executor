package com.color.nextflow.tieredbatch

import software.amazon.awssdk.services.batch.model.JobDetail
import software.amazon.awssdk.services.batch.model.JobStatus

class BatchFailureClassifier {
    enum Failure { INTERRUPTION, INFRASTRUCTURE, ORDINARY, UNRECOVERABLE }

    private static final List<String> TRANSIENT = [
        'connectionreseterror', 'connection reset by peer', 'connection aborted',
        'connection closed', 'connection refused', 'timed out', 'timeout',
        'could not connect to the endpoint', 'endpointconnectionerror',
        'temporary failure in name resolution', 'slowdown', 'throttling',
        'serviceunavailable', 'internalerror', 'internal server error',
        'status code: 500', 'status code: 502', 'status code: 503', 'status code: 504'
    ]
    private static final List<String> PERMANENT = [
        'accessdenied', 'access denied', 'unauthorized', 'forbidden', 'nosuchkey',
        'nosuchbucket', 'not found', '(404)', 'signaturedoesnotmatch',
        'invalidaccesskeyid', 'expiredtoken', 'unable to locate credentials'
    ]

    static Failure classify(JobDetail job, boolean spotTier, String diagnostics = '') {
        if (job?.status() != JobStatus.FAILED)
            return Failure.ORDINARY
        def reason = job.statusReason() ?: ''
        if (reason.startsWith('MISCONFIGURATION:') || reason.contains('Job killed by NF'))
            return Failure.UNRECOVERABLE
        if (spotTier && (reason.startsWith('Host EC2') || job.attempts().any { it.statusReason()?.startsWith('Host EC2') }))
            return Failure.INTERRUPTION
        def backendReasons = [reason, job.container()?.reason() ?: ''] + job.attempts().collect {
            "${it.statusReason() ?: ''} ${it.container()?.reason() ?: ''}"
        }
        if (backendReasons.any { isTransientContainerFailure(it) } || isTransientTransfer(diagnostics))
            return Failure.INFRASTRUCTURE
        return Failure.ORDINARY
    }

    private static boolean isTransientContainerFailure(String reason) {
        def lower = reason.toLowerCase(Locale.ROOT)
        return (lower.contains('cannotpullcontainer') || lower.contains('resourceinitializationerror')) && transientReason(lower)
    }

    private static boolean isTransientTransfer(String diagnostics) {
        def transfers = diagnostics.readLines().collect { it.toLowerCase(Locale.ROOT) }.findAll { lower ->
            boolean transfer = (lower.contains('download failed:') || lower.contains('upload failed:')) && lower.contains('s3://')
            boolean awsOperation = lower.contains('fatal error:') && lower.contains('when calling the') &&
                (lower.contains('getobject') || lower.contains('putobject') || lower.contains('uploadpart') || lower.contains('headobject') || lower.contains('listobjects'))
            transfer || awsOperation
        }
        return !transfers.any { line -> PERMANENT.any { line.contains(it) } } && transfers.any { transientReason(it) }
    }

    private static boolean transientReason(String reason) {
        return !PERMANENT.any { reason.contains(it) } && TRANSIENT.any { reason.contains(it) }
    }
}
