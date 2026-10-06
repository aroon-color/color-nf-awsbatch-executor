package com.color.nextflow.tieredbatch

import software.amazon.awssdk.services.batch.model.JobDetail
import software.amazon.awssdk.services.batch.model.JobStatus

class BatchFailureClassifier {
    enum Failure { INTERRUPTION, ORDINARY, UNRECOVERABLE }

    static Failure classify(JobDetail job, boolean spotTier) {
        if (job?.status() != JobStatus.FAILED)
            return Failure.ORDINARY
        def reason = job.statusReason() ?: ''
        if (reason.startsWith('MISCONFIGURATION:') || reason.contains('Job killed by NF'))
            return Failure.UNRECOVERABLE
        if (spotTier && (reason.startsWith('Host EC2') || job.attempts().any { it.statusReason()?.startsWith('Host EC2') }))
            return Failure.INTERRUPTION
        return Failure.ORDINARY
    }
}
