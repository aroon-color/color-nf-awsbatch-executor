package com.color.nextflow.tieredbatch

import groovy.json.JsonSlurper
import java.time.Duration
import java.time.Instant
import nextflow.cloud.aws.AwsClientFactory
import nextflow.cloud.aws.config.AwsConfig
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.services.ecs.EcsClient
import software.amazon.awssdk.services.ec2.Ec2Client
import software.amazon.awssdk.services.ecs.model.DescribeContainerInstancesRequest
import software.amazon.awssdk.services.ec2.model.*
import software.amazon.awssdk.services.pricing.PricingClient
import software.amazon.awssdk.services.pricing.model.Filter
import software.amazon.awssdk.services.pricing.model.GetProductsRequest

/** Read-only metadata collection runs outside the task monitor; missing prices remain unknown. */
class BatchMetadataResolver implements AutoCloseable {
    private final factory
    private final EcsClient ecs
    private final Ec2Client ec2
    private final PricingClient pricing
    private final Map<String, Map> instances = [:]
    private final Map<String, BigDecimal> prices = [:]

    private static class ReportingClients extends AwsClientFactory {
        ReportingClients(Map config) { super(new AwsConfig(config)) }
        def credentials() { getCredentialsProvider0() }
    }

    BatchMetadataResolver(Map awsConfig) {
        factory = new ReportingClients(awsConfig)
        def limits = ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofSeconds(5)).apiCallAttemptTimeout(Duration.ofSeconds(3)).build()
        ecs = EcsClient.builder().region(Region.of(factory.region())).credentialsProvider(factory.credentials()).overrideConfiguration(limits).build()
        ec2 = Ec2Client.builder().region(Region.of(factory.region())).credentialsProvider(factory.credentials()).overrideConfiguration(limits).build()
        pricing = PricingClient.builder().region(Region.US_EAST_1).credentialsProvider(factory.credentials())
            .overrideConfiguration(ClientOverrideConfiguration.builder().apiCallTimeout(Duration.ofSeconds(5)).apiCallAttemptTimeout(Duration.ofSeconds(3)).build()).build()
    }

    BatchMetadataResolver(EcsClient ecs, Ec2Client ec2, PricingClient pricing) {
        this.factory = null
        this.ecs = ecs
        this.ec2 = ec2
        this.pricing = pricing
    }

    Map resolve(Map row) {
        if (!row.containerInstanceArn || !row.ecsTaskArn) return [:]
        String arn = row.containerInstanceArn
        Map host = instances.get(arn)
        if (!host) {
            String cluster = (row.ecsTaskArn as String).split(':task/')[1].split('/')[0]
            def container = ecs.describeContainerInstances(DescribeContainerInstancesRequest.builder().cluster(cluster).containerInstances(arn).build()).containerInstances().find()
            if (!container) return [:]
            host = [instanceId: container.ec2InstanceId(), market: row.market ?: 'UNKNOWN']
            try {
                def instance = ec2.describeInstances(DescribeInstancesRequest.builder().instanceIds(container.ec2InstanceId()).build()).reservations().collectMany { it.instances() }.find()
                if (instance) {
                    host.putAll([instanceType: instance.instanceTypeAsString(), availabilityZone: instance.placement().availabilityZone(),
                        market: instance.instanceLifecycleAsString() == 'spot' ? 'SPOT' : 'ON_DEMAND'])
                    def type = ec2.describeInstanceTypes(DescribeInstanceTypesRequest.builder().instanceTypesWithStrings(instance.instanceTypeAsString()).build()).instanceTypes().find()
                    host.putAll([hostVcpus: type?.vCpuInfo()?.defaultVCpus(), hostMemoryMiB: type?.memoryInfo()?.sizeInMiB()])
                }
            }
            catch (RuntimeException error) { host.metadataError = error.class.simpleName }
            instances.put(arn, host)
        }
        Map result = new LinkedHashMap(host)
        try {
            if (!result.instanceType) return result
            result.hourlyRateUsd = price(row + host)
        }
        catch (RuntimeException error) { result.pricingError = error.class.simpleName }
        return result
    }

    private BigDecimal price(Map row) {
        if (row.market == 'SPOT') {
            if (!row.startedAt) return null
            def response = ec2.describeSpotPriceHistory(DescribeSpotPriceHistoryRequest.builder()
                .instanceTypesWithStrings(row.instanceType as String).availabilityZone(row.availabilityZone as String)
                .productDescriptions('Linux/UNIX').startTime(Instant.ofEpochMilli(row.startedAt as long))
                .endTime(Instant.ofEpochMilli(row.startedAt as long)).maxResults(100).build())
            def effective = response.spotPriceHistory().findAll { it.timestamp().toEpochMilli() <= row.startedAt }.max { it.timestamp() }
            return effective ? new BigDecimal(effective.spotPrice()) : null
        }
        String key = "${row.region}:${row.instanceType}"
        if (prices.containsKey(key)) return prices.get(key)
        def attributes = [regionCode: row.region, instanceType: row.instanceType, operatingSystem: 'Linux', tenancy: 'Shared', preInstalledSw: 'NA', capacitystatus: 'Used', productFamily: 'Compute Instance']
        def filters = attributes.collect { field, value -> Filter.builder().type('TERM_MATCH').field(field).value(value as String).build() }
        def products = pricing.getProducts(GetProductsRequest.builder().serviceCode('AmazonEC2').filters(filters).maxResults(100).build()).priceList()
        Set<BigDecimal> rates = [] as Set
        for (String product : products) {
            def document = new JsonSlurper().parseText(product)
            document.terms?.OnDemand?.values()?.each { term ->
                term.priceDimensions.values().findAll { it.unit == 'Hrs' && it.beginRange == '0' }.each { dimension ->
                    rates.add(new BigDecimal(dimension.pricePerUnit.USD as String))
                }
            }
        }
        BigDecimal rate = rates.size() == 1 ? rates.first() : null
        prices.put(key, rate)
        return rate
    }

    @Override void close() { pricing.close(); ecs.close(); ec2.close() }
}
