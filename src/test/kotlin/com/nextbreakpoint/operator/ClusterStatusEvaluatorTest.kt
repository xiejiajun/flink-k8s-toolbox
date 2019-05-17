package com.nextbreakpoint.operator

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.operator.model.*
import io.kubernetes.client.models.V1ObjectMeta
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1PersistentVolumeClaimSpec
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

@RunWith(JUnitPlatform::class)
@Tag("slow")
class ClusterStatusEvaluatorTest {
    private val evaluator = ClusterStatusEvaluator()

    private val baseClusterConfig = baseClusterConfig()

    @Test
    fun `should return valid for all resources when passing base configuration`() {
        val targetClusterConfig = baseClusterConfig()

        val targetResources = createClusterResources(targetClusterConfig)

        val actualStatus = evaluator.status(targetClusterConfig, targetResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.VALID)
    }

    @Test
    fun `should return missing when the jobmanager service is not present`() {
        val expectedResources = createClusterResources(baseClusterConfig).withJobManagerService(null)

        val actualStatus = evaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing when the sidecar deployment is not present`() {
        val expectedResources = createClusterResources(baseClusterConfig).withSidecarDeployment(null)

        val actualStatus = evaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing when the jobmanager stateful set is not present`() {
        val expectedResources = createClusterResources(baseClusterConfig).withJobManagerStatefulSet(null)

        val actualStatus = evaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing when the taskmanager stateful set is not present`() {
        val expectedResources = createClusterResources(baseClusterConfig).withTaskManagerStatefulSet(null)

        val actualStatus = evaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing when the jobmanager persistent volume claim is not present`() {
        val expectedResources = createClusterResources(baseClusterConfig).withJobManagerPersistenVolumeClaim(null)

        val actualStatus = evaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing when the taskmanager persistent volume claim is not present`() {
        val expectedResources = createClusterResources(baseClusterConfig).withTaskManagerPersistenVolumeClaim(null)

        val actualStatus = evaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.MISSING)
    }

    private fun printStatus(clusterStatus: ClusterStatus) {
        clusterStatus.jobmanagerService.second.forEach { println("jobmanager service: ${it}") }

        clusterStatus.sidecarDeployment.second.forEach { println("sidecar deployment: ${it}") }

        clusterStatus.jobmanagerStatefulSet.second.forEach { println("jobmanager stateful set: ${it}") }

        clusterStatus.taskmanagerStatefulSet.second.forEach { println("taskmanager stateful set: ${it}") }

        clusterStatus.jobmanagerPersistentVolumeClaim.second.forEach { println("jobmanager persistent volume claim: ${it}") }

        clusterStatus.taskmanagerPersistentVolumeClaim.second.forEach { println("taskmanager persistent volume claim: ${it}") }
    }

    private fun createPersistentVolumeClaim(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        storageConfig: StorageConfig,
        role: String
    ): V1PersistentVolumeClaim? {
        val persistentVolumeClaim = V1PersistentVolumeClaim()

        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", descriptor.environment)

        val clusterLabel = Pair("cluster", descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val roleLabel = Pair("role", role)

        val labels = mapOf(ownerLabel, clusterLabel, componentLabel, roleLabel, environmentLabel)

        persistentVolumeClaim.metadata = V1ObjectMeta()

        persistentVolumeClaim.metadata.labels(labels)

        persistentVolumeClaim.spec = V1PersistentVolumeClaimSpec()

        persistentVolumeClaim.spec.setStorageClassName(storageConfig.storageClass)

        return persistentVolumeClaim
    }

    private fun createClusterResources(targetClusterConfig: ClusterConfig): ClusterResources {
        val targetResources = ClusterResourcesBuilder("flink-operator", targetClusterConfig).build()

        return ClusterResources(
            jobmanagerService = targetResources.jobmanagerService,
            sidecarDeployment = targetResources.sidecarDeployment,
            jobmanagerStatefulSet = targetResources.jobmanagerStatefulSet,
            taskmanagerStatefulSet = targetResources.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = createPersistentVolumeClaim(
                "flink-operator",
                targetClusterConfig.descriptor,
                targetClusterConfig.jobmanager.storage,
                "jobmanager"
            ),
            taskmanagerPersistentVolumeClaim = createPersistentVolumeClaim(
                "flink-operator",
                targetClusterConfig.descriptor,
                targetClusterConfig.taskmanager.storage,
                "jobmanager"
            )
        )
    }

    private fun baseClusterConfig(): ClusterConfig {
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