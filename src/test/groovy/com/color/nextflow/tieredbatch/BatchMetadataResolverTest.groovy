package com.color.nextflow.tieredbatch

import java.time.Instant
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ecs.model.*
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ec2.model.*
import software.amazon.awssdk.services.pricing.PricingClient
import software.amazon.awssdk.services.pricing.model.*
import spock.lang.Specification

class BatchMetadataResolverTest extends Specification {
    def ecs = Mock(EcsClient)
    def ec2 = Mock(Ec2Client)
    def pricing = Mock(PricingClient)
    def row = [containerInstanceArn: 'arn:container/cluster/host', ecsTaskArn: 'arn:task/cluster/task',
        region: 'us-east-1', instanceType: 'm5.large', startedAt: 1000L]

    def 'resolve maps ECS to physical EC2 and caches identity and on-demand rate'() {
        given:
        def resolver = new BatchMetadataResolver(ecs, ec2, pricing)

        when:
        def first = resolver.resolve(row)
        def second = resolver.resolve(row)

        then:
        1 * ecs.describeContainerInstances(_ as DescribeContainerInstancesRequest) >> DescribeContainerInstancesResponse.builder()
            .containerInstances(ContainerInstance.builder().ec2InstanceId('i-123').build()).build()
        1 * ec2.describeInstances(_ as DescribeInstancesRequest) >> DescribeInstancesResponse.builder().reservations(
            Reservation.builder().instances(Instance.builder().instanceId('i-123').instanceType('m5.large')
                .placement(Placement.builder().availabilityZone('us-east-1a').build()).build()).build()).build()
        1 * ec2.describeInstanceTypes(_ as DescribeInstanceTypesRequest) >> DescribeInstanceTypesResponse.builder().instanceTypes(
            InstanceTypeInfo.builder().vCpuInfo(VCpuInfo.builder().defaultVCpus(2).build())
                .memoryInfo(MemoryInfo.builder().sizeInMiB(8192L).build()).build()).build()
        1 * pricing.getProducts(_ as GetProductsRequest) >> GetProductsResponse.builder().priceList(
            '{"terms":{"OnDemand":{"sku":{"priceDimensions":{"rate":{"unit":"Hrs","beginRange":"0","pricePerUnit":{"USD":"0.096"}}}}}}}').build()
        first.instanceId == 'i-123'
        first.market == 'ON_DEMAND'
        first.hostVcpus == 2
        first.hostMemoryMiB == 8192
        first.hourlyRateUsd == 0.096G
        second == first
    }

    def 'spot price uses availability zone and effective rate at job start'() {
        given:
        def resolver = new BatchMetadataResolver(ecs, ec2, pricing)

        when:
        def result = resolver.resolve(row)

        then:
        1 * ecs.describeContainerInstances(_ as DescribeContainerInstancesRequest) >> DescribeContainerInstancesResponse.builder()
            .containerInstances(ContainerInstance.builder().ec2InstanceId('i-spot').build()).build()
        1 * ec2.describeInstances(_ as DescribeInstancesRequest) >> DescribeInstancesResponse.builder().reservations(
            Reservation.builder().instances(Instance.builder().instanceId('i-spot').instanceType('m5.large').instanceLifecycle('spot')
                .placement(Placement.builder().availabilityZone('us-east-1a').build()).build()).build()).build()
        1 * ec2.describeInstanceTypes(_ as DescribeInstanceTypesRequest) >> DescribeInstanceTypesResponse.builder().instanceTypes(
            InstanceTypeInfo.builder().vCpuInfo(VCpuInfo.builder().defaultVCpus(2).build())
                .memoryInfo(MemoryInfo.builder().sizeInMiB(8192L).build()).build()).build()
        1 * ec2.describeSpotPriceHistory({ it.availabilityZone() == 'us-east-1a' && it.startTime() == Instant.ofEpochMilli(1000) }) >>
            DescribeSpotPriceHistoryResponse.builder().spotPriceHistory(SpotPrice.builder().spotPrice('0.03').timestamp(Instant.EPOCH).build()).build()
        0 * pricing._
        result.hourlyRateUsd == 0.03G
        result.market == 'SPOT'
    }

    def 'missing container metadata makes no AWS requests'() {
        expect:
        new BatchMetadataResolver(ecs, ec2, pricing).resolve([:]) == [:]
    }
}
