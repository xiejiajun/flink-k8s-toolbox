package com.nextbreakpoint.command

import com.github.tomakehurst.wiremock.client.WireMock.*
import com.github.tomakehurst.wiremock.junit.WireMockRule
import com.github.tomakehurst.wiremock.matching.EqualToJsonPattern
import com.nextbreakpoint.DefaultWebClientFactory
import com.nextbreakpoint.WebClientFactory
import com.nextbreakpoint.model.*
import com.nextbreakpoint.operator.TestFactory
import io.vertx.core.json.Json
import io.vertx.rxjava.ext.web.client.WebClient
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.assertThrows
import java.lang.Exception

@RunWith(JUnitPlatform::class)
class PostCommandTest {
    private val CLUSTER = TestFactory.aCluster()

    private val CLUSTER_JSON = Json.encode(TestFactory.aCluster())

    private val DESCRIPTOR_JSON = Json.encode(CLUSTER.descriptor)

    private val JOBDESCRIPTOR_JSON = Json.encode(JobDescriptor(CLUSTER.descriptor, "001"))

    private val JOBCANCELPARAMS_JSON = Json.encode(JobCancelParams(JobDescriptor(CLUSTER.descriptor, "001"), "/tmp", true))

    private val JOBSCALEPARAMS_JSON = Json.encode(JobScaleParams(JobDescriptor(CLUSTER.descriptor, "001"), 2))

    private val JOBSLISTPARAMS_JSON = Json.encode(JobsListParams(CLUSTER.descriptor, true))

    private val JOBRUNPARAMS_JSON = Json.encode(JobRunParams(CLUSTER.descriptor, CLUSTER.sidecar))

    private val TASKMANAGERDESCRIPTOR_JSON = Json.encode(TaskManagerDescriptor(CLUSTER.descriptor, "00002"))

    private fun configureStub(path: String) {
        stubFor(
            post(urlEqualTo(path))
                .willReturn(
                    aResponse()
                        .withStatus(200)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"response\":\"ok\"}")
                )
        )
    }

    private var wireMockRule: WireMockRule? = null

    @BeforeEach
    fun setup() {
        wireMockRule = WireMockRule(8089)
        wireMockRule?.start()
    }

    @AfterEach
    fun teardown() {
        wireMockRule?.stop()
    }

    @Test
    fun `should invoke api with given payload`() {
        configureFor("localhost", 8089)

        stubFor(post(urlEqualTo("/test"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"response\":\"ok\"}")));

        PostCommand<String>(DefaultWebClientFactory).run(ApiParams("localhost", 8089), "/test", "payload")

        verify(1, postRequestedFor(urlEqualTo("/test"))
            .withRequestBody(EqualToJsonPattern("'payload'", true, true)))
    }

    @Test
    fun `should throw when factory throws exception`() {
        val testFactory = object : WebClientFactory {
            override fun create(params: ApiParams): WebClient {
                throw Exception()
            }
        }

        assertThrows<RuntimeException> {  PostCommand<String>(testFactory).run(ApiParams("localhost", 8089), "/test", "payload") }
    }

    @Test
    fun `should invoke cluster create`() {
        configureFor("localhost", 8089)

        configureStub("/cluster/create")

        PostCommandClusterCreate().run(ApiParams("localhost", 8089), CLUSTER)

        verify(1, postRequestedFor(urlEqualTo("/cluster/create"))
            .withRequestBody(EqualToJsonPattern(CLUSTER_JSON, true, true)))
    }

    @Test
    fun `should invoke cluster delete`() {
        configureFor("localhost", 8089)

        configureStub("/cluster/delete")

        PostCommandClusterDelete().run(ApiParams("localhost", 8089), CLUSTER.descriptor)

        verify(1, postRequestedFor(urlEqualTo("/cluster/delete"))
            .withRequestBody(EqualToJsonPattern(DESCRIPTOR_JSON, true, true)))
    }

    @Test
    fun `should invoke job details`() {
        configureFor("localhost", 8089)

        configureStub("/job/details")

        PostCommandJobDetails().run(ApiParams("localhost", 8089), JobDescriptor(CLUSTER.descriptor, "001"))

        verify(1, postRequestedFor(urlEqualTo("/job/details"))
            .withRequestBody(EqualToJsonPattern(JOBDESCRIPTOR_JSON, true, true)))
    }

    @Test
    fun `should invoke job cancel`() {
        configureFor("localhost", 8089)

        configureStub("/job/cancel")

        PostCommandJobCancel().run(ApiParams("localhost", 8089), JobCancelParams(JobDescriptor(CLUSTER.descriptor, "001"), "/tmp", true))

        verify(1, postRequestedFor(urlEqualTo("/job/cancel"))
            .withRequestBody(EqualToJsonPattern(JOBCANCELPARAMS_JSON, true, true)))
    }

    @Test
    fun `should invoke job metrics`() {
        configureFor("localhost", 8089)

        configureStub("/job/metrics")

        PostCommandJobMetrics().run(ApiParams("localhost", 8089), JobDescriptor(CLUSTER.descriptor, "001"))

        verify(1, postRequestedFor(urlEqualTo("/job/metrics"))
            .withRequestBody(EqualToJsonPattern(JOBDESCRIPTOR_JSON, true, true)))
    }

    @Test
    fun `should invoke job scale`() {
        configureFor("localhost", 8089)

        configureStub("/job/scale")

        PostCommandJobScale().run(ApiParams("localhost", 8089), JobScaleParams(JobDescriptor(CLUSTER.descriptor, "001"), 2))

        verify(1, postRequestedFor(urlEqualTo("/job/scale"))
            .withRequestBody(EqualToJsonPattern(JOBSCALEPARAMS_JSON, true, true)))
    }

    @Test
    fun `should invoke jobs list`() {
        configureFor("localhost", 8089)

        configureStub("/jobs/list")

        PostCommandJobsList().run(ApiParams("localhost", 8089), JobsListParams(CLUSTER.descriptor, true))

        verify(1, postRequestedFor(urlEqualTo("/jobs/list"))
            .withRequestBody(EqualToJsonPattern(JOBSLISTPARAMS_JSON, true, true)))
    }

    @Test
    fun `should invoke job run`() {
        configureFor("localhost", 8089)

        configureStub("/job/run")

        PostCommandJobRun().run(ApiParams("localhost", 8089), JobRunParams(CLUSTER.descriptor, CLUSTER.sidecar))

        verify(1, postRequestedFor(urlEqualTo("/job/run"))
            .withRequestBody(EqualToJsonPattern(JOBRUNPARAMS_JSON, true, true)))
    }

    @Test
    fun `should invoke jobmanager metrics`() {
        configureFor("localhost", 8089)

        configureStub("/jobmanager/metrics")

        PostCommandJobManagerMetrics().run(ApiParams("localhost", 8089), CLUSTER.descriptor)

        verify(1, postRequestedFor(urlEqualTo("/jobmanager/metrics"))
            .withRequestBody(EqualToJsonPattern(DESCRIPTOR_JSON, true, true)))
    }

    @Test
    fun `should invoke taskmanager metrics`() {
        configureFor("localhost", 8089)

        configureStub("/taskmanager/metrics")

        PostCommandTaskManagerMetrics().run(ApiParams("localhost", 8089), TaskManagerDescriptor(CLUSTER.descriptor, "00002"))

        verify(1, postRequestedFor(urlEqualTo("/taskmanager/metrics"))
            .withRequestBody(EqualToJsonPattern(TASKMANAGERDESCRIPTOR_JSON, true, true)))
    }

    @Test
    fun `should invoke taskmanager details`() {
        configureFor("localhost", 8089)

        configureStub("/taskmanager/details")

        PostCommandTaskManagerDetails().run(ApiParams("localhost", 8089), TaskManagerDescriptor(CLUSTER.descriptor, "00002"))

        verify(1, postRequestedFor(urlEqualTo("/taskmanager/details"))
            .withRequestBody(EqualToJsonPattern(TASKMANAGERDESCRIPTOR_JSON, true, true)))
    }

    @Test
    fun `should invoke taskmanagers list`() {
        configureFor("localhost", 8089)

        configureStub("/taskmanagers/list")

        PostCommandTaskManagersList().run(ApiParams("localhost", 8089), CLUSTER.descriptor)

        verify(1, postRequestedFor(urlEqualTo("/taskmanagers/list"))
            .withRequestBody(EqualToJsonPattern(DESCRIPTOR_JSON, true, true)))
    }
}