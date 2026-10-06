package com.color.nextflow.tieredbatch

import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model.DescribeComputeEnvironmentsRequest
import software.amazon.awssdk.services.batch.model.DescribeJobQueuesRequest

class BatchQueueValidator {
    static void validate(BatchClient client, Map<String, String> mappings, String region) {
        mappings.each { spot, demand ->
            validateQueue(client, spot, 'SPOT', region)
            validateQueue(client, demand, 'EC2', region)
        }
    }

    private static void validateQueue(BatchClient client, String queue, String expectedType, String region) {
        def response = client.describeJobQueues(DescribeJobQueuesRequest.builder().jobQueues(queue).build())
        if (response.jobQueues().size() != 1)
            throw new IllegalArgumentException("Cannot resolve AWS Batch queue '${queue}'")
        def detail = response.jobQueues().first()
        if (detail.stateAsString() != 'ENABLED' || detail.statusAsString() != 'VALID' || detail.jobQueueArn().split(':')[3] != region)
            throw new IllegalArgumentException("Queue '${queue}' must be enabled, valid, and in region ${region}")
        def names = detail.computeEnvironmentOrder().collect { it.computeEnvironment() }
        if (names.isEmpty())
            throw new IllegalArgumentException("Queue '${queue}' has no compute environments")
        def environments = client.describeComputeEnvironments(DescribeComputeEnvironmentsRequest.builder().computeEnvironments(names).build()).computeEnvironments()
        if (environments.size() != names.size() || environments.any {
            it.stateAsString() != 'ENABLED' || it.statusAsString() != 'VALID' || it.computeResources()?.typeAsString() != expectedType
        })
            throw new IllegalArgumentException("Queue '${queue}' must contain only enabled, valid ${expectedType} compute environments")
    }
}
