package com.color.nextflow.tieredbatch

import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model.*
import spock.lang.Specification

class BatchQueueValidatorTest extends Specification {
    def 'queue type validation rejects mixed or disabled destinations'() {
        given:
        def client = Stub(BatchClient) {
            describeJobQueues(_ as DescribeJobQueuesRequest) >> { DescribeJobQueuesRequest request ->
                String name = request.jobQueues().first()
                DescribeJobQueuesResponse.builder().jobQueues(JobQueueDetail.builder().jobQueueName(name)
                    .jobQueueArn("arn:aws:batch:us-east-1:000000000000:job-queue/${name}").state('ENABLED').status('VALID')
                    .computeEnvironmentOrder(ComputeEnvironmentOrder.builder().order(1).computeEnvironment(name).build()).build()).build()
            }
            describeComputeEnvironments(_ as DescribeComputeEnvironmentsRequest) >> { DescribeComputeEnvironmentsRequest request ->
                String name = request.computeEnvironments().first()
                DescribeComputeEnvironmentsResponse.builder().computeEnvironments(ComputeEnvironmentDetail.builder()
                    .state(name == 'spot' ? 'ENABLED' : destinationState).status('VALID')
                    .computeResources(ComputeResource.builder().type(name == 'spot' ? 'SPOT' : destinationType).build()).build()).build()
            }
        }

        when:
        BatchQueueValidator.validate(client, [spot: 'demand'], 'us-east-1')

        then:
        thrown(IllegalArgumentException)

        where:
        destinationType | destinationState
        'SPOT'          | 'ENABLED'
        'EC2'           | 'DISABLED'
        'FARGATE'       | 'ENABLED'
    }
}
