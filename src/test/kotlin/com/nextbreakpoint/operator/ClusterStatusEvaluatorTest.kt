package com.nextbreakpoint.operator

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.operator.model.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

@RunWith(JUnitPlatform::class)
@Tag("slow")
class ClusterStatusEvaluatorTest {
    private val evaluator = ClusterStatusEvaluator()

    @Test
    fun `should return missing when the jobmanager service is not present`() {
        val targetClusterConfig = aClusterConfig()

        val targetResources = ClusterResourcesBuilder("flink-operator", targetClusterConfig).build()

        val resources = ClusterResources(
            jobmanagerService = null,
            sidecarDeployment = targetResources.sidecarDeployment,
            jobmanagerStatefulSet = targetResources.jobmanagerStatefulSet,
            taskmanagerStatefulSet = targetResources.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = targetResources.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = targetResources.taskmanagerPersistentVolumeClaim
        )

        val actualStatus = evaluator.status(targetClusterConfig, resources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.MISSING)
        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.VALID)
    }

    private fun printStatus(clusterStatus: ClusterStatus) {
        clusterStatus.jobmanagerService.second.forEach { println("jobmanager service: ${it}") }

        clusterStatus.sidecarDeployment.second.forEach { println("sidecar deployment: ${it}") }

        clusterStatus.jobmanagerStatefulSet.second.forEach { println("jobmanager stateful set: ${it}") }

        clusterStatus.taskmanagerStatefulSet.second.forEach { println("taskmanager stateful set: ${it}") }

        clusterStatus.jobmanagerPersistentVolumeClaim.second.forEach { println("jobmanager persistent volume claim: ${it}") }

        clusterStatus.taskmanagerPersistentVolumeClaim.second.forEach { println("taskmanager persistent volume claim: ${it}") }
    }

    private fun aClusterConfig(): ClusterConfig {
        return ClusterConfig(
            descriptor = ClusterDescriptor(
                namespace = "testNamespace",
                name = "testCluster",
                environment = "testEnvironment"
            ),
            jobmanager = JobManagerConfig(
                image = "flink:1.7.2",
                pullPolicy = "Always",
                pullSecrets = "somesecrets",
                serviceMode = "NodePort",
                serviceAccount = "testServiceAccount",
                environmentVariables = listOf(EnvironmentVariable("key", "value")),
                storage = StorageConfig(
                    size = 100,
                    storageClass = "testStorageClass"
                ),
                resources = ResourcesConfig(
                    cpus = 1.0f,
                    memory = 500
                )
            ),
            taskmanager = TaskManagerConfig(
                image = "flink:1.7.2",
                pullPolicy = "Always",
                pullSecrets = "somesecrets",
                serviceAccount = "testServiceAccount",
                taskSlots = 2,
                replicas = 1,
                environmentVariables = listOf(EnvironmentVariable("key", "value")),
                storage = StorageConfig(
                    size = 100,
                    storageClass = "testStorageClass"
                ),
                resources = ResourcesConfig(
                    cpus = 1.0f,
                    memory = 1000
                )
            ),
            sidecar = SidecarConfig(
                image = "sidecar:1.0",
                pullPolicy = "Always",
                serviceAccount = "testServiceAccount",
                pullSecrets = "somesecrets",
                jarPath = "test.jar",
                className = "test.TestJob",
                savepoint = "somesavepoint",
                arguments = "--key=value",
                parallelism = 1
            )
        )
    }
}