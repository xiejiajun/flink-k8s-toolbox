package com.nextbreakpoint.operator

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.operator.model.Storage
import com.nextbreakpoint.operator.model.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith

@RunWith(JUnitPlatform::class)
class DefaultClusterResourcesFactoryTest {
    private val descriptor = ClusterDescriptor(
        name = "myCluster",
        namespace = "myNamespace",
        environment = "myEnvironment"
    )

    @Test
    fun `should create jobmanager service`() {
        val service = DefaultClusterResourcesFactory.createJobManagerService("myself", descriptor, "NodePort")

        assertThat(service.metadata?.name).isEqualTo("flink-jobmanager-myCluster")

        val labels = service.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("cluster")).isEqualTo("myCluster")
        assertThat(labels?.get("environment")).isEqualTo("myEnvironment")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        assertThat(service.spec?.type).isEqualTo("NodePort")

        val ports = service.spec?.ports
        assertThat(ports).hasSize(4)
        assertThat(ports?.get(0)?.name).isEqualTo("ui")
        assertThat(ports?.get(1)?.name).isEqualTo("rpc")
        assertThat(ports?.get(2)?.name).isEqualTo("blob")
        assertThat(ports?.get(3)?.name).isEqualTo("query")

        val selector = service.spec?.selector
        assertThat(selector).hasSize(5)
        assertThat(selector?.get("owner")).isNotNull()
        assertThat(selector?.get("cluster")).isNotNull()
        assertThat(selector?.get("environment")).isNotNull()
        assertThat(selector?.get("component")).isNotNull()
        assertThat(selector?.get("role")).isNotNull()
    }

    @Test
    fun `should create sidecar deployment when submitting job`() {
        val param1 = "--key1=value1"
        val param2 = "--key2=value2"

        val sidecarConfig = Sidecar(
            image = "sidecar:1.0",
            pullPolicy = "Always",
            serviceAccount = "testServiceAccount",
            pullSecrets = "somesecrets",
            jarPath = "test.jar",
            className = "test.TestJob",
            savepoint = "somesavepoint",
            arguments = "$param1 $param2",
            parallelism = 2
        )

        val deployment = DefaultClusterResourcesFactory.createSidecarDeployment("myself", descriptor, sidecarConfig)

        assertThat(deployment.metadata?.name).isEqualTo("flink-sidecar-myCluster")

        val labels = deployment.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("cluster")).isEqualTo("myCluster")
        assertThat(labels?.get("environment")).isEqualTo("myEnvironment")
        assertThat(labels?.get("component")).isEqualTo("flink")

        assertThat(deployment.spec?.replicas).isEqualTo(1)

        val matchLabels = deployment.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(4)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("cluster")).isNotNull()
        assertThat(matchLabels?.get("environment")).isNotNull()
        assertThat(matchLabels?.get("component")).isNotNull()

        val podSpec = deployment.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo(sidecarConfig.serviceAccount)
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(sidecarConfig.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(1)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.ports).isNull()
        assertThat(container?.imagePullPolicy).isEqualTo(sidecarConfig.pullPolicy)
        assertThat(container?.args).hasSize(11)
        assertThat(container?.args?.get(0)).isEqualTo("sidecar")
        assertThat(container?.args?.get(1)).isEqualTo("submit")
        assertThat(container?.args?.get(2)).isEqualTo("--namespace=${descriptor.namespace}")
        assertThat(container?.args?.get(3)).isEqualTo("--environment=${descriptor.environment}")
        assertThat(container?.args?.get(4)).isEqualTo("--cluster-name=${descriptor.name}")
        assertThat(container?.args?.get(5)).isEqualTo("--jar-path=${sidecarConfig.jarPath}")
        assertThat(container?.args?.get(6)).isEqualTo("--parallelism=${sidecarConfig.parallelism}")
        assertThat(container?.args?.get(7)).isEqualTo("--class-name=${sidecarConfig.className}")
        assertThat(container?.args?.get(8)).isEqualTo("--savepoint=${sidecarConfig.savepoint}")
        assertThat(container?.args?.get(9)).isEqualTo("--argument=$param1")
        assertThat(container?.args?.get(10)).isEqualTo("--argument=$param2")
        assertThat(container?.env).hasSize(3)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("FLINK_ENVIRONMENT")
    }

    @Test
    fun `should create sidecar deployment when watching job`() {
        val sidecarConfig = Sidecar(
            image = "sidecar:1.0",
            pullPolicy = "Always",
            serviceAccount = "testServiceAccount",
            pullSecrets = "somesecrets",
            jarPath = null,
            className = "test.TestJob",
            savepoint = "somesavepoint",
            arguments = "--key1=value1 --key2=value2",
            parallelism = 2
        )

        val deployment = DefaultClusterResourcesFactory.createSidecarDeployment("myself", descriptor, sidecarConfig)

        assertThat(deployment.metadata?.name).isEqualTo("flink-sidecar-myCluster")

        val labels = deployment.metadata?.labels
        assertThat(labels).hasSize(4)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("cluster")).isEqualTo("myCluster")
        assertThat(labels?.get("environment")).isEqualTo("myEnvironment")
        assertThat(labels?.get("component")).isEqualTo("flink")

        assertThat(deployment.spec?.replicas).isEqualTo(1)

        val matchLabels = deployment.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(4)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("cluster")).isNotNull()
        assertThat(matchLabels?.get("environment")).isNotNull()
        assertThat(matchLabels?.get("component")).isNotNull()

        val podSpec = deployment.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo(sidecarConfig.serviceAccount)
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(sidecarConfig.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(1)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.ports).isNull()
        assertThat(container?.image).isEqualTo(sidecarConfig.image)
        assertThat(container?.imagePullPolicy).isEqualTo(sidecarConfig.pullPolicy)
        assertThat(container?.args).hasSize(5)
        assertThat(container?.args?.get(0)).isEqualTo("sidecar")
        assertThat(container?.args?.get(1)).isEqualTo("watch")
        assertThat(container?.args?.get(2)).isEqualTo("--namespace=${descriptor.namespace}")
        assertThat(container?.args?.get(3)).isEqualTo("--environment=${descriptor.environment}")
        assertThat(container?.args?.get(4)).isEqualTo("--cluster-name=${descriptor.name}")
        assertThat(container?.env).hasSize(3)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("FLINK_ENVIRONMENT")
    }

    @Test
    fun `should create jobmanager statefulset`() {
        val jobmanagerConfig = JobManager(
            image = "flink:1.7.2",
            pullPolicy = "Always",
            pullSecrets = "somesecrets",
            serviceMode = "ClusterIP",
            serviceAccount = "testServiceAccount",
            environmentVariables = listOf(EnvironmentVariable("key", "value")),
            storage = Storage(
                size = 100,
                storageClass = "testStorageClass"
            ),
            resources = Resources(
                cpus = 1.0f,
                memory = 500
            )
        )

        val statefulset = DefaultClusterResourcesFactory.createJobManagerStatefulSet("myself", descriptor, jobmanagerConfig)

        assertThat(statefulset.metadata?.name).isEqualTo("flink-jobmanager-myCluster")

        val labels = statefulset.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("cluster")).isEqualTo("myCluster")
        assertThat(labels?.get("environment")).isEqualTo("myEnvironment")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("jobmanager")

        assertThat(statefulset.spec?.replicas).isEqualTo(1)
        assertThat(statefulset.spec?.updateStrategy).isNotNull()
        assertThat(statefulset.spec?.serviceName).isEqualTo("jobmanager")
        assertThat(statefulset.spec?.selector).isNotNull()

        val matchLabels = statefulset.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(5)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("cluster")).isNotNull()
        assertThat(matchLabels?.get("environment")).isNotNull()
        assertThat(matchLabels?.get("component")).isNotNull()
        assertThat(matchLabels?.get("role")).isNotNull()

        assertThat(statefulset.spec?.volumeClaimTemplates).hasSize(1)
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName).isEqualTo(jobmanagerConfig.storage.storageClass)
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests?.get("storage")?.number?.toInt()).isEqualTo(jobmanagerConfig.storage.size)

        val podSpec = statefulset.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo(jobmanagerConfig.serviceAccount)
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(jobmanagerConfig.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(2)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.image).isEqualTo(jobmanagerConfig.image)
        assertThat(container?.imagePullPolicy).isEqualTo(jobmanagerConfig.pullPolicy)
        assertThat(container?.ports).hasSize(4)
        assertThat(container?.ports?.get(0)?.name).isEqualTo("ui")
        assertThat(container?.ports?.get(1)?.name).isEqualTo("rpc")
        assertThat(container?.ports?.get(2)?.name).isEqualTo("blob")
        assertThat(container?.ports?.get(3)?.name).isEqualTo("query")
        assertThat(container?.args).hasSize(1)
        assertThat(container?.args?.get(0)).isEqualTo("jobmanager")
        assertThat(container?.env).hasSize(6)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("FLINK_ENVIRONMENT")
        assertThat(container?.env?.get(3)?.name).isEqualTo("FLINK_JM_HEAP")
        assertThat(container?.env?.get(4)?.name).isEqualTo("JOB_MANAGER_RPC_ADDRESS")
        assertThat(container?.env?.get(5)?.name).isEqualTo("key")
        assertThat(container?.env?.get(5)?.value).isEqualTo("value")
        assertThat(container?.volumeMounts).hasSize(1)
        assertThat(container?.volumeMounts?.get(0)?.name).isEqualTo("jobmanager")
        assertThat(container?.resources?.limits?.get("cpu")?.number?.toFloat()).isEqualTo(jobmanagerConfig.resources.cpus)
        assertThat(container?.resources?.requests?.get("memory")?.number?.toInt()).isEqualTo(jobmanagerConfig.resources.memory * 1024 * 1024)
    }

    @Test
    fun `should create taskmanager statefulset`() {
        val taskmanagerConfig = TaskManager(
            image = "flink:1.7.2",
            pullPolicy = "Always",
            pullSecrets = "somesecrets",
            serviceAccount = "testServiceAccount",
            taskSlots = 2,
            replicas = 4,
            environmentVariables = listOf(EnvironmentVariable("key", "value")),
            storage = Storage(
                size = 100,
                storageClass = "testStorageClass"
            ),
            resources = Resources(
                cpus = 1.0f,
                memory = 1000
            )
        )

        val statefulset = DefaultClusterResourcesFactory.createTaskManagerStatefulSet("myself", descriptor, taskmanagerConfig)

        assertThat(statefulset.metadata?.name).isEqualTo("flink-taskmanager-myCluster")

        val labels = statefulset.metadata?.labels
        assertThat(labels).hasSize(5)
        assertThat(labels?.get("owner")).isEqualTo("myself")
        assertThat(labels?.get("cluster")).isEqualTo("myCluster")
        assertThat(labels?.get("environment")).isEqualTo("myEnvironment")
        assertThat(labels?.get("component")).isEqualTo("flink")
        assertThat(labels?.get("role")).isEqualTo("taskmanager")

        assertThat(statefulset.spec?.replicas).isEqualTo(taskmanagerConfig.replicas)
        assertThat(statefulset.spec?.updateStrategy).isNotNull()
        assertThat(statefulset.spec?.serviceName).isEqualTo("taskmanager")
        assertThat(statefulset.spec?.selector).isNotNull()

        val matchLabels = statefulset.spec?.selector?.matchLabels
        assertThat(matchLabels).hasSize(5)
        assertThat(matchLabels?.get("owner")).isNotNull()
        assertThat(matchLabels?.get("cluster")).isNotNull()
        assertThat(matchLabels?.get("environment")).isNotNull()
        assertThat(matchLabels?.get("component")).isNotNull()
        assertThat(matchLabels?.get("role")).isNotNull()

        assertThat(statefulset.spec?.volumeClaimTemplates).hasSize(1)
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.storageClassName).isEqualTo(taskmanagerConfig.storage.storageClass)
        assertThat(statefulset.spec?.volumeClaimTemplates?.get(0)?.spec?.resources?.requests?.get("storage")?.number?.toInt()).isEqualTo(taskmanagerConfig.storage.size)

        val podSpec = statefulset.spec?.template?.spec
        assertThat(podSpec?.serviceAccountName).isEqualTo(taskmanagerConfig.serviceAccount)
        assertThat(podSpec?.imagePullSecrets).hasSize(1)
        assertThat(podSpec?.imagePullSecrets?.get(0)?.name).isEqualTo(taskmanagerConfig.pullSecrets)
        assertThat(podSpec?.affinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).isNotNull()
        assertThat(podSpec?.affinity?.podAntiAffinity?.preferredDuringSchedulingIgnoredDuringExecution).hasSize(2)
        assertThat(podSpec?.containers).hasSize(1)

        val container = podSpec?.containers?.get(0)
        assertThat(container?.image).isEqualTo(taskmanagerConfig.image)
        assertThat(container?.imagePullPolicy).isEqualTo(taskmanagerConfig.pullPolicy)
        assertThat(container?.ports).hasSize(2)
        assertThat(container?.ports?.get(0)?.name).isEqualTo("data")
        assertThat(container?.ports?.get(1)?.name).isEqualTo("ipc")
        assertThat(container?.args).hasSize(1)
        assertThat(container?.args?.get(0)).isEqualTo("taskmanager")
        assertThat(container?.env).hasSize(7)
        assertThat(container?.env?.get(0)?.name).isEqualTo("POD_NAME")
        assertThat(container?.env?.get(1)?.name).isEqualTo("POD_NAMESPACE")
        assertThat(container?.env?.get(2)?.name).isEqualTo("FLINK_ENVIRONMENT")
        assertThat(container?.env?.get(3)?.name).isEqualTo("FLINK_TM_HEAP")
        assertThat(container?.env?.get(4)?.name).isEqualTo("JOB_MANAGER_RPC_ADDRESS")
        assertThat(container?.env?.get(5)?.name).isEqualTo("TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
        assertThat(container?.env?.get(5)?.value).isEqualTo("${taskmanagerConfig.taskSlots}")
        assertThat(container?.env?.get(6)?.name).isEqualTo("key")
        assertThat(container?.env?.get(6)?.value).isEqualTo("value")
        assertThat(container?.volumeMounts).hasSize(1)
        assertThat(container?.volumeMounts?.get(0)?.name).isEqualTo("taskmanager")
        assertThat(container?.resources?.limits?.get("cpu")?.number?.toFloat()).isEqualTo(taskmanagerConfig.resources.cpus)
        assertThat(container?.resources?.requests?.get("memory")?.number?.toInt()).isEqualTo(taskmanagerConfig.resources.memory * 1024 * 1024)
    }
}