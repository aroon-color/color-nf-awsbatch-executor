package com.color.nextflow.tieredbatch

import software.amazon.awssdk.services.batch.model.AttemptDetail
import software.amazon.awssdk.services.batch.model.JobDetail
import spock.lang.Specification

class BatchFailureClassifierTest extends Specification {
    def 'host reasons classify only failed Spot-tier jobs as interruptions'() {
        expect:
        BatchFailureClassifier.classify(JobDetail.builder().status(status).statusReason(reason)
            .attempts(AttemptDetail.builder().statusReason(attemptReason).build()).build(), spot) == expected

        where:
        status      | reason                                      | attemptReason                           | spot  | expected
        'FAILED'    | 'Host EC2 (instance i-test) terminated.'     | ''                                      | true  | BatchFailureClassifier.Failure.INTERRUPTION
        'FAILED'    | ''                                          | 'Host EC2 (instance i-test) terminated.' | true  | BatchFailureClassifier.Failure.INTERRUPTION
        'FAILED'    | 'Host EC2 (instance i-test) terminated.'     | ''                                      | false | BatchFailureClassifier.Failure.ORDINARY
        'SUCCEEDED' | 'Host EC2 (instance i-test) terminated.'     | ''                                      | true  | BatchFailureClassifier.Failure.ORDINARY
        'FAILED'    | 'OutOfMemoryError'                          | ''                                      | true  | BatchFailureClassifier.Failure.ORDINARY
        'FAILED'    | 'Essential container exited with code 143' | ''                                      | true  | BatchFailureClassifier.Failure.ORDINARY
        'FAILED'    | 'MISCONFIGURATION:JOB_RESOURCE_REQUIREMENT' | ''                                      | true  | BatchFailureClassifier.Failure.UNRECOVERABLE
        'FAILED'    | 'Job killed by NF'                          | ''                                      | true  | BatchFailureClassifier.Failure.UNRECOVERABLE
    }

    def 'transient transfer errors from worker diagnostics are infrastructure failures'() {
        given:
        def job = JobDetail.builder().status('FAILED').statusReason('Essential container in task exited').build()

        expect:
        BatchFailureClassifier.classify(job, true, diagnostics) == expected

        where:
        diagnostics | expected
        "download failed: s3://work/reference.fa to ./reference.fa (\"Connection broken: ConnectionResetError(104, 'Connection reset by peer')\", ConnectionResetError(104, 'Connection reset by peer'))" | BatchFailureClassifier.Failure.INFRASTRUCTURE
        'upload failed: ./result.txt to s3://work/result.txt Read timeout on endpoint URL' | BatchFailureClassifier.Failure.INFRASTRUCTURE
        'fatal error: An error occurred (SlowDown) when calling the GetObject operation' | BatchFailureClassifier.Failure.INFRASTRUCTURE
        'download failed: s3://work/input to ./input An error occurred (AccessDenied): Access Denied' | BatchFailureClassifier.Failure.ORDINARY
        'download failed: s3://work/input to ./input An error occurred (404): Not Found' | BatchFailureClassifier.Failure.ORDINARY
        'upload failed: ./result to s3://work/result AccessDenied: timeout policy' | BatchFailureClassifier.Failure.ORDINARY
        'Application ConnectionResetError: Connection reset by peer' | BatchFailureClassifier.Failure.ORDINARY
        'download failed: s3://work/input to ./input [Errno 2] No such file or directory' | BatchFailureClassifier.Failure.ORDINARY
        'download failed: s3://work/input to ./input Connection reset by peer\nupload failed: ./result to s3://work/result AccessDenied: Access Denied' | BatchFailureClassifier.Failure.ORDINARY
        '' | BatchFailureClassifier.Failure.ORDINARY
    }

    def 'container startup network failures are infrastructure but permanent image errors are not'() {
        expect:
        BatchFailureClassifier.classify(JobDetail.builder().status('FAILED').statusReason(reason).build(), true) == expected

        where:
        reason | expected
        'CannotPullContainerError: request canceled: Client.Timeout exceeded' | BatchFailureClassifier.Failure.INFRASTRUCTURE
        'ResourceInitializationError: unable to pull secrets: connection reset by peer' | BatchFailureClassifier.Failure.INFRASTRUCTURE
        'CannotPullContainerError: unauthorized' | BatchFailureClassifier.Failure.ORDINARY
        'CannotPullContainerError: image not found' | BatchFailureClassifier.Failure.ORDINARY
        'OutOfMemoryError: container killed' | BatchFailureClassifier.Failure.ORDINARY
    }
}
