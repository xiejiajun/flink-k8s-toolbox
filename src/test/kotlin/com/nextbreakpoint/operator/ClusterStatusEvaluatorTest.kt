package com.nextbreakpoint.operator

import com.nextbreakpoint.common.TestFactory
import com.nextbreakpoint.common.model.Descriptor
import com.nextbreakpoint.operator.model.ClusterStatus
import com.nextbreakpoint.operator.model.ResourceStatus
import com.nextbreakpoint.operator.model.Storage
import com.nextbreakpoint.operator.model.*
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

@RunWith(JUnitPlatform::class)
class ClusterStatusEvaluatorTest {
    private val statusEvaluator = ClusterStatusEvaluator()

    private val baseClusterConfig = TestFactory.aCluster()

    @Test
    fun `should return all valid resources when creating resources from base configuration`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.VALID)
        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.VALID)
    }

    @Test
    fun `should return missing resource when the jobmanager service is not present`() {
        val expectedResources = createTestClusterResources(baseClusterConfig).withJobManagerService(null)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the sidecar deployment is not present`() {
        val expectedResources = createTestClusterResources(baseClusterConfig).withSidecarDeployment(null)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the jobmanager statefulset is not present`() {
        val expectedResources = createTestClusterResources(baseClusterConfig).withJobManagerStatefulSet(null)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the taskmanager statefulset is not present`() {
        val expectedResources = createTestClusterResources(baseClusterConfig).withTaskManagerStatefulSet(null)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the jobmanager persistent volume claim is not present`() {
        val expectedResources = createTestClusterResources(baseClusterConfig).withJobManagerPersistenVolumeClaim(null)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return missing resource when the taskmanager persistent volume claim is not present`() {
        val expectedResources = createTestClusterResources(baseClusterConfig).withTaskManagerPersistenVolumeClaim(null)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.MISSING)
    }

    @Test
    fun `should return divergent resource when the jobmanager service does not have the expected labels`() {
        val jobmanagerService = V1Service()

        jobmanagerService.metadata = V1ObjectMeta()
        jobmanagerService.metadata.name = "testCluster"
        jobmanagerService.metadata.namespace = "testNamespace"
        jobmanagerService.metadata.labels = mapOf()

        jobmanagerService.spec = V1ServiceSpec()
        jobmanagerService.spec.type = "ClusterIP"

        val expectedResources = createTestClusterResources(baseClusterConfig).withJobManagerService(jobmanagerService)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerService.second).hasSize(3)
    }

    @Test
    fun `should return divergent resource when the jobmanager service does not have the expected service mode`() {
        val jobmanagerService = V1Service()

        val labels = createLabels(baseClusterConfig.descriptor, "flink-operator", "jobmanager")

        jobmanagerService.metadata = V1ObjectMeta()
        jobmanagerService.metadata.name = "testCluster"
        jobmanagerService.metadata.namespace = "testNamespace"
        jobmanagerService.metadata.labels = labels

        jobmanagerService.spec = V1ServiceSpec()
        jobmanagerService.spec.type = "NodePort"

        val expectedResources = createTestClusterResources(baseClusterConfig).withJobManagerService(jobmanagerService)

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerService.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerService.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected labels`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(3)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected service account`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.serviceAccountName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected pull secrets`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.imagePullSecrets = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected pull secrets name`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.imagePullSecrets?.get(0)?.name = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected volume claims`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.volumeClaimTemplates = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected volume claims storage class`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected volume claims storage size`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests = mapOf("storage" to Quantity("10"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have containers`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected container image`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.image = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected container pull policy`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.imagePullPolicy = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected container cpu limits`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.resources?.limits?.set("cpu", Quantity("2.0"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected container memory limits`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.resources?.requests?.set("memory", Quantity("100Mi"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the expected container environment variables`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.env = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(6)
    }

    @Test
    fun `should return divergent resource when the jobmanager statefulset does not have the internal expected container environment variables`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.env = listOf(V1EnvVar().name("key").value("value"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerStatefulSet.second).hasSize(5)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected labels`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(3)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected service account`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.serviceAccountName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected pull secrets`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.imagePullSecrets = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected pull secrets name`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.imagePullSecrets?.get(0)?.name = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected volume claims`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.volumeClaimTemplates = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected volume claims storage class`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected volume claims storage size`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests = mapOf("storage" to Quantity("10"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have containers`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected container image`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.image = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected container pull policy`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.imagePullPolicy = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected container cpu limits`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.resources?.limits?.set("cpu", Quantity("2.0"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected container memory limits`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.resources?.requests?.set("memory", Quantity("100Mi"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected container environment variables`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.env = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(7)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the internal expected container environment variables`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.template?.spec?.containers?.get(0)?.env = listOf(V1EnvVar().name("key").value("value"))

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(6)
    }

    @Test
    fun `should return divergent resource when the taskmanager statefulset does not have the expected number of replicas`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerStatefulSet?.spec?.replicas = 4

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerStatefulSet.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerStatefulSet.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the jobmanager persistent volume claim does not have the expected labels`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerPersistentVolumeClaim?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.second).hasSize(3)
    }

    @Test
    fun `should return divergent resource when the jobmanager persistent volume claim does not have the expected storage class`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.jobmanagerPersistentVolumeClaim?.spec?.storageClassName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.jobmanagerPersistentVolumeClaim.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the taskmanager persistent volume claim does not have the expected labels`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerPersistentVolumeClaim?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.second).hasSize(3)
    }

    @Test
    fun `should return divergent resource when the taskmanager persistent volume claim does not have the expected storage class`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.taskmanagerPersistentVolumeClaim?.spec?.storageClassName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.taskmanagerPersistentVolumeClaim.second).hasSize(1)
    }

    private fun printStatus(clusterStatus: ClusterStatus) {
        clusterStatus.jobmanagerService.second.forEach { println("jobmanager service: ${it}") }

        clusterStatus.sidecarDeployment.second.forEach { println("sidecar deployment: ${it}") }

        clusterStatus.jobmanagerStatefulSet.second.forEach { println("jobmanager statefulset: ${it}") }

        clusterStatus.taskmanagerStatefulSet.second.forEach { println("taskmanager statefulset: ${it}") }

        clusterStatus.jobmanagerPersistentVolumeClaim.second.forEach { println("jobmanager persistent volume claim: ${it}") }

        clusterStatus.taskmanagerPersistentVolumeClaim.second.forEach { println("taskmanager persistent volume claim: ${it}") }
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected labels`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.metadata?.labels = mapOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(2)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected service account`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.serviceAccountName = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected pull secrets`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.imagePullSecrets = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected pull secrets name`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.imagePullSecrets?.get(0)?.name = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have containers`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected container image`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.image = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected container pull policy`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.imagePullPolicy = "xxx"

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected container arguments`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.args = listOf()

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected sidecar argument`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.args = listOf("xxx", "submit")

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected sidecar arguments`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.args = listOf("sidecar", "xxx")

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(1)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected sidecar submit arguments`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.args = listOf("sidecar", "submit")

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(8)
    }

    @Test
    fun `should return divergent resource when the sidecar deployment does not have the expected sidecar watch arguments`() {
        val expectedResources = createTestClusterResources(baseClusterConfig)

        expectedResources.sidecarDeployment?.spec?.template?.spec?.containers?.get(0)?.args = listOf("sidecar", "watch")

        val actualStatus = statusEvaluator.status(baseClusterConfig, expectedResources)

        printStatus(actualStatus)

        assertThat(actualStatus.sidecarDeployment.first).isEqualTo(ResourceStatus.DIVERGENT)
        assertThat(actualStatus.sidecarDeployment.second).hasSize(7)
    }

    private fun createPersistentVolumeClaim(
        clusterOwner: String,
        descriptor: Descriptor,
        storage: Storage,
        role: String
    ): V1PersistentVolumeClaim? {
        val persistentVolumeClaim = V1PersistentVolumeClaim()

        val labels = createLabels(descriptor, clusterOwner, role)

        persistentVolumeClaim.metadata = V1ObjectMeta()
        persistentVolumeClaim.metadata.labels = labels

        persistentVolumeClaim.spec = V1PersistentVolumeClaimSpec()
        persistentVolumeClaim.spec.storageClassName = storage.storageClass

        return persistentVolumeClaim
    }

    private fun createLabels(
        descriptor: Descriptor,
        clusterOwner: String,
        role: String
    ): Map<String, String> {
        val componentLabel = Pair("component", "flink")

        val clusterLabel = Pair("cluster", descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val roleLabel = Pair("role", role)

        return mapOf(ownerLabel, clusterLabel, componentLabel, roleLabel)
    }

    private fun createTestClusterResources(targetCluster: Cluster): ClusterResources {
        val targetResources = ClusterResourcesBuilder(DefaultClusterResourcesFactory, "flink-operator", targetCluster).build()

        return ClusterResources(
            jobmanagerService = targetResources.jobmanagerService,
            sidecarDeployment = targetResources.sidecarDeployment,
            jobmanagerStatefulSet = targetResources.jobmanagerStatefulSet,
            taskmanagerStatefulSet = targetResources.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = createPersistentVolumeClaim(
                "flink-operator",
                targetCluster.descriptor,
                targetCluster.jobmanager.storage,
                "jobmanager"
            ),
            taskmanagerPersistentVolumeClaim = createPersistentVolumeClaim(
                "flink-operator",
                targetCluster.descriptor,
                targetCluster.taskmanager.storage,
                "taskmanager"
            )
        )
    }
}