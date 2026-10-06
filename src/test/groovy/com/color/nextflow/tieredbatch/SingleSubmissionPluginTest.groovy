package com.color.nextflow.tieredbatch

import com.sun.net.httpserver.HttpServer
import java.util.concurrent.atomic.AtomicInteger
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.retries.StandardRetryStrategy
import software.amazon.awssdk.services.batch.BatchClient
import software.amazon.awssdk.services.batch.model.BatchException
import software.amazon.awssdk.services.batch.model.SubmitJobRequest
import spock.lang.Specification

class SingleSubmissionPluginTest extends Specification {
    def 'submit request disables SDK retries even when client permits them'() {
        given:
        def calls = new AtomicInteger()
        def server = HttpServer.create(new InetSocketAddress('127.0.0.1', 0), 0)
        server.createContext('/') { exchange ->
            calls.incrementAndGet()
            byte[] response = '{"message":"Accepted job; response lost"}'.bytes
            exchange.responseHeaders.add('Content-Type', 'application/json')
            exchange.sendResponseHeaders(500, response.length)
            exchange.responseBody.withCloseable { it.write(response) }
        }
        server.start()
        def client = BatchClient.builder().region(Region.US_EAST_1)
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create('fixture', 'fixture')))
            .endpointOverride(new URI("http://127.0.0.1:${server.address.port}"))
            .overrideConfiguration(ClientOverrideConfiguration.builder()
                .retryStrategy(StandardRetryStrategy.builder().maxAttempts(3).build()).build()).build()

        when:
        client.submitJob(SubmitJobRequest.builder().jobName('fixture').jobQueue('spot').jobDefinition('fixture')
            .overrideConfiguration(AwsRequestOverrideConfiguration.builder().addPlugin(new SingleSubmissionPlugin()).build()).build())

        then:
        thrown(BatchException)
        calls.get() == 1

        cleanup:
        client?.close()
        server?.stop(0)
    }
}
