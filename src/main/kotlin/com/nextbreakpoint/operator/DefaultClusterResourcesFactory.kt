package com.nextbreakpoint.operator

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.operator.model.*
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.*

object DefaultClusterResourcesFactory : ClusterResourcesFactory {
    override fun createJobManagerService(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        serviceMode: String
    ): V1Service {
        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", descriptor.environment)

        val clusterLabel = Pair("cluster", descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val jobmanagerLabel = Pair("role", "jobmanager")//TODO remove

        val serviceLabels = mapOf(ownerLabel, clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

        val srvPort8081 = createServicePort(8081, "ui")
        val srvPort6123 = createServicePort(6123, "rpc")
        val srvPort6124 = createServicePort(6124, "blob")
        val srvPort6125 = createServicePort(6125, "query")

        val jobmanagerServiceSpec = V1ServiceSpec()
            .ports(
                listOf(
                    srvPort8081,
                    srvPort6123,
                    srvPort6124,
                    srvPort6125
                )
            )
            .selector(serviceLabels)
            .type(serviceMode)

        val jobmanagerServiceMetadata =
            createObjectMeta("flink-jobmanager-${descriptor.name}", serviceLabels)

        val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerServiceMetadata)

        return jobmanagerService
    }

    override fun createSidecarDeployment(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        sidecarConfig: SidecarConfig
    ): V1Deployment {
        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", descriptor.environment)

        val clusterLabel = Pair("cluster", descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val sidecarLabels = mapOf(ownerLabel, clusterLabel, componentLabel, environmentLabel)

        val podNameEnvVar = createEnvVarFromField(
            "POD_NAME",
            "metadata.name"
        )

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE",
            "metadata.namespace"
        )

        val environmentEnvVar = createEnvVar(
            "FLINK_ENVIRONMENT",
            descriptor.environment
        )

        val sidecarSelector = V1LabelSelector().matchLabels(sidecarLabels)

        val sidecarAffinity = createSidecarAffinity(sidecarSelector)

        val arguments = mutableListOf<String>()

        if (sidecarConfig.jarPath != null) {
            arguments.addAll(
                listOf(
                    "sidecar",
                    "submit",
                    "--namespace=${descriptor.namespace}",
                    "--environment=${descriptor.environment}",
                    "--cluster-name=${descriptor.name}",
                    "--jar-path=${sidecarConfig.jarPath}",
                    "--parallelism=${sidecarConfig.parallelism}"
                )
            )

            if (sidecarConfig.className != null) {
                arguments.add("--class-name=${sidecarConfig.className}")
            }

            if (sidecarConfig.savepoint != null) {
                arguments.add("--savepoint=${sidecarConfig.savepoint}")
            }

            sidecarConfig.arguments?.split(" ")?.forEach { argument ->
                arguments.add("--argument=$argument")
            }
        } else {
            arguments.addAll(
                listOf(
                    "sidecar",
                    "watch",
                    "--namespace=${descriptor.namespace}",
                    "--environment=${descriptor.environment}",
                    "--cluster-name=${descriptor.name}"
                )
            )
        }

        val sidecar = V1Container()
            .image(sidecarConfig.image)
            .imagePullPolicy(sidecarConfig.pullPolicy)
            .name("flink-sidecar")
            .args(arguments)
            .env(
                listOf(
                    podNameEnvVar,
                    podNamespaceEnvVar,
                    environmentEnvVar
                )
            )
            .resources(createSidecarResourceRequirements())

        val sidecarPullSecrets = if (sidecarConfig.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(sidecarConfig.pullSecrets)
            )
        } else null

        val sidecarPodSpec = V1PodSpec()
            .containers(
                listOf(sidecar)
            )
            .serviceAccountName(sidecarConfig.serviceAccount)
            .imagePullSecrets(sidecarPullSecrets)
            .affinity(sidecarAffinity)

        val sidecarMetadata =
            createObjectMeta("flink-sidecar-${descriptor.name}", sidecarLabels)

        val sidecarDeployment = V1Deployment()
            .metadata(sidecarMetadata)
            .spec(
                V1DeploymentSpec()
                    .replicas(1)
                    .template(
                        V1PodTemplateSpec()
                            .spec(sidecarPodSpec)
                            .metadata(sidecarMetadata)
                    )
                    .selector(sidecarSelector)
            )

        return sidecarDeployment
    }

    override fun createJobManagerStatefulSet(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        jobmanagerConfig: JobManagerConfig
    ): V1StatefulSet {
        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", descriptor.environment)

        val clusterLabel = Pair("cluster", descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val jobmanagerLabel = Pair("role", "jobmanager")

        val taskmanagerLabel = Pair("role", "taskmanager")

        val jobmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

        val taskmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, taskmanagerLabel, environmentLabel)

        val jobmanagerVolumeMount = createVolumeMount("jobmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val port8081 = createContainerPort(8081, "ui")
        val port6123 = createContainerPort(6123, "rpc")
        val port6124 = createContainerPort(6124, "blob")
        val port6125 = createContainerPort(6125, "query")

        val podNameEnvVar = createEnvVarFromField(
            "POD_NAME",
            "metadata.name"
        )

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE",
            "metadata.namespace"
        )

        val environmentEnvVar = createEnvVar(
            "FLINK_ENVIRONMENT",
            descriptor.environment
        )

        val jobManagerHeapEnvVar = createEnvVar(
            "FLINK_JM_HEAP",
            jobmanagerConfig.resources.memory.toString()
        )

        val rpcAddressEnvVar = createEnvVar(
            "JOB_MANAGER_RPC_ADDRESS",
            "flink-jobmanager-${descriptor.name}"
        )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val jobmanagerAffinity = createAffinity(
            jobmanagerSelector,
            taskmanagerSelector
        )

        val jobmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            jobManagerHeapEnvVar,
            rpcAddressEnvVar
        )

        val jobmanagerUserVariables = jobmanagerConfig.environmentVariables.map { createEnvVar(it.name, it.value) }.toList()

        jobmanagerVariables.addAll(jobmanagerUserVariables)

        val jobmanager = V1Container()
            .image(jobmanagerConfig.image)
            .imagePullPolicy(jobmanagerConfig.pullPolicy)
            .name("flink-jobmanager")
            .args(
                listOf("jobmanager")
            )
            .ports(
                listOf(
                    port8081,
                    port6123,
                    port6124,
                    port6125
                )
            )
            .volumeMounts(listOf(jobmanagerVolumeMount))
            .env(
                jobmanagerVariables
            )
            .resources(createResourceRequirements(jobmanagerConfig.resources))

        val jobmanagerPullSecrets = if (jobmanagerConfig.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(jobmanagerConfig.pullSecrets)
            )
        } else null

        val jobmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(jobmanager)
            )
            .serviceAccountName(jobmanagerConfig.serviceAccount)
            .imagePullSecrets(jobmanagerPullSecrets)
            .affinity(jobmanagerAffinity)

        val jobmanagerMetadata =
            createObjectMeta("flink-jobmanager-${descriptor.name}", jobmanagerLabels)

        val jobmanagerVolumeClaim =
            createPersistentVolumeClaimSpec(jobmanagerConfig.storage)

        val jobmanagerStatefulSet = V1StatefulSet()
            .metadata(jobmanagerMetadata)
            .spec(
                V1StatefulSetSpec()
                    .replicas(1)
                    .template(
                        V1PodTemplateSpec()
                            .spec(jobmanagerPodSpec)
                            .metadata(jobmanagerMetadata)
                    )
                    .updateStrategy(updateStrategy)
                    .serviceName("jobmanager")
                    .selector(jobmanagerSelector)
                    .addVolumeClaimTemplatesItem(
                        V1PersistentVolumeClaim()
                            .spec(jobmanagerVolumeClaim)
                            .metadata(
                                V1ObjectMeta()
                                    .name("jobmanager")
                                    .labels(jobmanagerLabels)
                            )
                    )
            )

        return jobmanagerStatefulSet
    }

    override fun createTaskManagerStatefulSet(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        taskmanagerConfig: TaskManagerConfig
    ): V1StatefulSet {
        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", descriptor.environment)

        val clusterLabel = Pair("cluster", descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val jobmanagerLabel = Pair("role", "jobmanager")

        val taskmanagerLabel = Pair("role", "taskmanager")

        val jobmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

        val taskmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, taskmanagerLabel, environmentLabel)

        val taskmanagerVolumeMount = createVolumeMount("taskmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val port6121 = createContainerPort(6121, "data")
        val port6122 = createContainerPort(6122, "ipc")

        val podNameEnvVar = createEnvVarFromField(
            "POD_NAME",
            "metadata.name"
        )

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE",
            "metadata.namespace"
        )

        val environmentEnvVar = createEnvVar(
            "FLINK_ENVIRONMENT",
            descriptor.environment
        )

        val taskManagerHeapEnvVar = createEnvVar(
            "FLINK_TM_HEAP",
            taskmanagerConfig.resources.memory.toString()
        )

        val rpcAddressEnvVar = createEnvVar(
            "JOB_MANAGER_RPC_ADDRESS",
            "flink-jobmanager-${descriptor.name}"
        )

        val numberOfTaskSlotsEnvVar = createEnvVar(
            "TASK_MANAGER_NUMBER_OF_TASK_SLOTS",
            taskmanagerConfig.taskSlots.toString()
        )

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val taskmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            taskManagerHeapEnvVar,
            rpcAddressEnvVar,
            numberOfTaskSlotsEnvVar
        )

        val taskmanagerUserVariables = taskmanagerConfig.environmentVariables.map { createEnvVar(it.name, it.value) }.toList()

        taskmanagerVariables.addAll(taskmanagerUserVariables)

        val taskmanager = V1Container()
            .image(taskmanagerConfig.image)
            .imagePullPolicy(taskmanagerConfig.pullPolicy)
            .name("flink-taskmanager")
            .args(
                listOf("taskmanager")
            )
            .ports(
                listOf(
                    port6121,
                    port6122
                )
            )
            .volumeMounts(
                listOf(taskmanagerVolumeMount)
            )
            .env(
                taskmanagerVariables
            )
            .resources(createResourceRequirements(taskmanagerConfig.resources))

        val taskmanagerAffinity =
            createAffinity(jobmanagerSelector, taskmanagerSelector)

        val taskmanagerPullSecrets = if (taskmanagerConfig.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(taskmanagerConfig.pullSecrets)
            )
        } else null

        val taskmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(taskmanager)
            )
            .serviceAccountName(taskmanagerConfig.serviceAccount)
            .imagePullSecrets(taskmanagerPullSecrets)
            .affinity(taskmanagerAffinity)

        val taskmanagerMetadata =
            createObjectMeta("flink-taskmanager-${descriptor.name}", taskmanagerLabels)

        val taskmanagerVolumeClaim =
            createPersistentVolumeClaimSpec(taskmanagerConfig.storage)

        val taskmanagerStatefulSet = V1StatefulSet()
            .metadata(taskmanagerMetadata)
            .spec(
                V1StatefulSetSpec()
                    .replicas(taskmanagerConfig.replicas)
                    .template(
                        V1PodTemplateSpec()
                            .spec(taskmanagerPodSpec)
                            .metadata(taskmanagerMetadata)
                    )
                    .updateStrategy(updateStrategy)
                    .serviceName("taskmanager")
                    .selector(taskmanagerSelector)
                    .addVolumeClaimTemplatesItem(
                        V1PersistentVolumeClaim()
                            .spec(taskmanagerVolumeClaim)
                            .metadata(
                                V1ObjectMeta()
                                    .name("taskmanager")
                                    .labels(taskmanagerLabels)
                            )
                    )
            )

        return taskmanagerStatefulSet
    }

    private fun createAffinity(jobmanagerSelector: V1LabelSelector?, taskmanagerSelector: V1LabelSelector?): V1Affinity = V1Affinity()
        .podAntiAffinity(
            V1PodAntiAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                listOf(
                    V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(jobmanagerSelector)
                    ),
                    V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(taskmanagerSelector)
                    )
                )
            )
        )

    private fun createSidecarAffinity(sidecarSelector: V1LabelSelector?): V1Affinity = V1Affinity()
        .podAffinity(
            V1PodAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                listOf(
                    V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(sidecarSelector)
                    )
                )
            )
        )

    private fun createPersistentVolumeClaimSpec(storageConfig: StorageConfig): V1PersistentVolumeClaimSpec = V1PersistentVolumeClaimSpec()
        .accessModes(listOf("ReadWriteOnce"))
        .storageClassName(storageConfig.storageClass)
        .resources(
            V1ResourceRequirements()
                .requests(
                    mapOf("storage" to Quantity(storageConfig.size.toString()))
                )
        )

    private fun createObjectMeta(name: String, jobmanagerLabels: Map<String, String>): V1ObjectMeta = V1ObjectMeta()
        .name(name)
        .labels(jobmanagerLabels)

    private fun createVolumeMount(name: String): V1VolumeMount = V1VolumeMount()
        .mountPath("/var/tmp/data")
        .subPath(name)
        .name(name)

    private fun createEnvVarFromField(name: String, fieldPath: String): V1EnvVar = V1EnvVar()
        .name(name)
        .valueFrom(
            V1EnvVarSource()
                .fieldRef(
                    V1ObjectFieldSelector().fieldPath(fieldPath)
                )
        )

    private fun createEnvVar(name: String, value: String): V1EnvVar = V1EnvVar()
        .name(name)
        .value(value)

    private fun createResourceRequirements(resourcesConfig: ResourcesConfig): V1ResourceRequirements = V1ResourceRequirements()
        .limits(
            mapOf(
                "cpu" to Quantity(resourcesConfig.cpus.toString()),
                "memory" to Quantity(resourcesConfig.memory.times(1.5).toString() + "Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity(resourcesConfig.cpus.div(4).toString()),
                "memory" to Quantity(resourcesConfig.memory.toString() + "Mi")
            )
        )

    private fun createSidecarResourceRequirements(): V1ResourceRequirements = V1ResourceRequirements()
        .limits(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("200Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("200Mi")
            )
        )

    private fun createServicePort(port: Int, name: String): V1ServicePort = V1ServicePort()
        .protocol("TCP")
        .port(port)
        .targetPort(IntOrString(name))
        .name(name)

    private fun createContainerPort(port: Int, name: String): V1ContainerPort = V1ContainerPort()
        .protocol("TCP")
        .containerPort(port)
        .name(name)
}