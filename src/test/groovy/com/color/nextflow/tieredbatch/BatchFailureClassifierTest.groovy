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
}
