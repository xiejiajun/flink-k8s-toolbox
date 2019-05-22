package com.nextbreakpoint.operator

import com.nextbreakpoint.model.V1FlinkClusterEnvVar
import com.nextbreakpoint.model.V1FlinkClusterSpec
import io.kubernetes.client.models.V1ObjectMeta
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

@RunWith(JUnitPlatform::class)
class ClusterBuilderTest {
    private val metadata = V1ObjectMeta()
        .name("testCluster")
        .namespace("testNamespace")

    private val spec = V1FlinkClusterSpec()
        .setClusterName("testCluster")
        .setEnvironment("testEnvironment")
        .setJobmanagerCpus(1f)
        .setJobmanagerMemory(500)
        .setJobmanagerServiceAccount("testServiceAccount")
        .setJobmanagerStorageClass("testStorageClass")
        .setJobmanagerStorageSize(100)
        .setJobmanagerEnvironmentVariables(listOf(V1FlinkClusterEnvVar().setName("key").setValue("value")))
        .setTaskmanagerCpus(1f)
        .setTaskmanagerMemory(1000)
        .setTaskmanagerReplicas(2)
        .setTaskmanagerServiceAccount("testServiceAccount")
        .setTaskmanagerStorageClass("testStorageClass")
        .setTaskmanagerStorageSize(100)
        .setTaskmanagerEnvironmentVariables(listOf(V1FlinkClusterEnvVar().setName("key").setValue("value")))
        .setServiceMode("ClusterIP")
        .setFlinkImage("flink:1.7.2")
        .setPullPolicy("Always")
        .setPullSecrets("somesecrets")
        .setSidecarImage("sidecar:1.0")
        .setSidecarClassName("test.TestJob")
        .setSidecarServiceAccount("testServiceAccount")
        .setSidecarSavepoint("somesavepoint")
        .setSidecarJarPath("test.jar")
        .setSidecarArguments(listOf("--key=value"))
        .setSidecarParallelism(1)

    private val expectedConfig = TestFactory.baseClusterConfig()

    @Test
    fun `should create config`() {
        val builder = ClusterConfigBuilder(
            metadata = metadata,
            spec = spec
        )

        val actualConfig = builder.build()

        assertThat(actualConfig).isEqualTo(expectedConfig)
    }
}