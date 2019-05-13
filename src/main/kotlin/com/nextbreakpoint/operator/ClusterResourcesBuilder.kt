package com.nextbreakpoint.operator

import com.nextbreakpoint.operator.model.ClusterConfig
import com.nextbreakpoint.operator.model.ClusterResources
import com.nextbreakpoint.operator.model.ResourcesConfig
import com.nextbreakpoint.operator.model.StorageConfig
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.*

class ClusterResourcesBuilder(
    private val clusterOwner: String,
    private val clusterConfig: ClusterConfig
) {
    fun build(): ClusterResources {
        val srvPort8081 = createServicePort(8081, "ui")
        val srvPort6123 = createServicePort(6123, "rpc")
        val srvPort6124 = createServicePort(6124, "blob")
        val srvPort6125 = createServicePort(6125, "query")

        val port8081 = createContainerPort(8081, "ui")
        val port6121 = createContainerPort(6121, "data")
        val port6122 = createContainerPort(6122, "ipc")
        val port6123 = createContainerPort(6123, "rpc")
        val port6124 = createContainerPort(6124, "blob")
        val port6125 = createContainerPort(6125, "query")

        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", clusterConfig.descriptor.environment)

        val clusterLabel = Pair("cluster", clusterConfig.descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val jobmanagerLabel = Pair("role", "jobmanager")

        val taskmanagerLabel = Pair("role", "taskmanager")

        val jobmanagerResources = clusterConfig.jobmanager.resources

        val taskmanagerResources = clusterConfig.taskmanager.resources

        val jobmanagerResourceRequirements = createResourceRequirements(jobmanagerResources)

        val taskmanagerResourceRequirements = createResourceRequirements(taskmanagerResources)

        val jobmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

        val taskmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, taskmanagerLabel, environmentLabel)

        val sidecarLabels = mapOf(ownerLabel, clusterLabel, componentLabel, environmentLabel)

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val sidecarSelector = V1LabelSelector().matchLabels(sidecarLabels)

        val environmentEnvVar = createEnvVar(
            "FLINK_ENVIRONMENT",
            clusterConfig.descriptor.environment
        )

        val jobManagerHeapEnvVar = createEnvVar(
            "FLINK_JM_HEAP",
            jobmanagerResources.memory.toString()
        )

        val taskManagerHeapEnvVar = createEnvVar(
            "FLINK_TM_HEAP",
            taskmanagerResources.memory.toString()
        )

        val numberOfTaskSlotsEnvVar = createEnvVar(
            "TASK_MANAGER_NUMBER_OF_TASK_SLOTS",
            clusterConfig.taskmanager.taskSlots.toString()
        )

        val podNameEnvVar =
            createEnvVarFromField("POD_NAME", "metadata.name")

        val podNamespaceEnvVar = createEnvVarFromField(
            "POD_NAMESPACE",
            "metadata.namespace"
        )

        val jobmanagerVolumeMount = createVolumeMount("jobmanager")

        val taskmanagerVolumeMount =
            createVolumeMount("taskmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val jobmanagerServiceSpec = V1ServiceSpec()
            .ports(
                listOf(
                    srvPort8081,
                    srvPort6123,
                    srvPort6124,
                    srvPort6125
                )
            )
            .selector(jobmanagerLabels)
            .type(clusterConfig.jobmanager.serviceMode)

        val jobmanagerServiceMetadata =
            createObjectMeta("flink-jobmanager-${clusterConfig.descriptor.name}", jobmanagerLabels)

        val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerServiceMetadata)

        val rpcAddressEnvVar = createEnvVar(
            "JOB_MANAGER_RPC_ADDRESS",
            "flink-jobmanager-${clusterConfig.descriptor.name}"
        )

        val jobmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            rpcAddressEnvVar,
            jobManagerHeapEnvVar
        )

        val jobmanagerUserVariables = clusterConfig.jobmanager.environmentVariables
            .map { createEnvVar(it.name, it.value) }.toList()

        jobmanagerVariables.addAll(jobmanagerUserVariables)

        val jobmanager = V1Container()
            .image(clusterConfig.jobmanager.image)
            .imagePullPolicy(clusterConfig.jobmanager.pullPolicy)
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
            .resources(jobmanagerResourceRequirements)

        val jobmanagerAffinity =
            createAffinity(jobmanagerSelector, taskmanagerSelector)

        val jobmanagerPullSecrets = if (clusterConfig.jobmanager.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(clusterConfig.jobmanager.pullSecrets)
            )
        } else null

        val jobmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(jobmanager)
            )
            .serviceAccountName(clusterConfig.jobmanager.serviceAccount)
            .imagePullSecrets(jobmanagerPullSecrets)
            .affinity(jobmanagerAffinity)

        val jobmanagerMetadata =
            createObjectMeta("flink-jobmanager-${clusterConfig.descriptor.name}", jobmanagerLabels)

        val jobmanagerVolumeClaim =
            createPersistentVolumeClaimSpec(clusterConfig.jobmanager.storage)

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

        val arguments = mutableListOf<String>()

        if (clusterConfig.sidecar.jarPath != null) {
            arguments.addAll(listOf(
                "sidecar",
                "submit",
                "--namespace=${clusterConfig.descriptor.namespace}",
                "--environment=${clusterConfig.descriptor.environment}",
                "--cluster-name=${clusterConfig.descriptor.name}",
                "--jar-path=${clusterConfig.sidecar.jarPath}",
                "--parallelism=${clusterConfig.sidecar.parallelism}"
            ))

            if (clusterConfig.sidecar.className != null) {
                arguments.add("--class-name=${clusterConfig.sidecar.className}")
            }

            if (clusterConfig.sidecar.savepoint != null) {
                arguments.add("--savepoint=${clusterConfig.sidecar.savepoint}")
            }

            clusterConfig.sidecar.arguments?.split(" ")?.forEach { argument ->
                arguments.add("--argument=$argument")
            }
        } else {
            arguments.addAll(listOf(
                "sidecar",
                "watch",
                "--namespace=${clusterConfig.descriptor.namespace}",
                "--environment=${clusterConfig.descriptor.environment}",
                "--cluster-name=${clusterConfig.descriptor.name}"
            ))
        }

        val sidecar = V1Container()
            .image(clusterConfig.sidecar.image)
            .imagePullPolicy(clusterConfig.sidecar.pullPolicy)
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

        val sidecarPullSecrets = if (clusterConfig.sidecar.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(clusterConfig.sidecar.pullSecrets)
            )
        } else null

        val sidecarPodSpec = V1PodSpec()
            .containers(
                listOf(sidecar)
            )
            .serviceAccountName(clusterConfig.sidecar.serviceAccount)
            .imagePullSecrets(sidecarPullSecrets)
            .affinity(jobmanagerAffinity)

        val sidecarMetadata =
            createObjectMeta("flink-sidecar-${clusterConfig.descriptor.name}", sidecarLabels)

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

        val taskmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            rpcAddressEnvVar,
            taskManagerHeapEnvVar,
            numberOfTaskSlotsEnvVar
        )

        val taskmanagerUserVariables = clusterConfig.taskmanager.environmentVariables
            .map { createEnvVar(it.name, it.value) }.toList()

        taskmanagerVariables.addAll(taskmanagerUserVariables)

        val taskmanager = V1Container()
            .image(clusterConfig.taskmanager.image)
            .imagePullPolicy(clusterConfig.taskmanager.pullPolicy)
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
            .resources(taskmanagerResourceRequirements)

        val taskmanagerAffinity =
            createAffinity(jobmanagerSelector, taskmanagerSelector)

        val taskmanagerPullSecrets = if (clusterConfig.taskmanager.pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(clusterConfig.taskmanager.pullSecrets)
            )
        } else null

        val taskmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(taskmanager)
            )
            .serviceAccountName(clusterConfig.taskmanager.serviceAccount)
            .imagePullSecrets(taskmanagerPullSecrets)
            .affinity(taskmanagerAffinity)

        val taskmanagerMetadata =
            createObjectMeta("flink-taskmanager--${clusterConfig.descriptor.name}", taskmanagerLabels)

        val taskmanagerVolumeClaim =
            createPersistentVolumeClaimSpec(clusterConfig.taskmanager.storage)

        val taskmanagerStatefulSet = V1StatefulSet()
            .metadata(taskmanagerMetadata)
            .spec(
                V1StatefulSetSpec()
                    .replicas(clusterConfig.taskmanager.replicas)
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

        return ClusterResources(
            jobmanagerService = jobmanagerService,
            sidecarDeployment = sidecarDeployment,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = null,
            taskmanagerPersistentVolumeClaim = null
        )
    }

    private fun createAffinity(jobmanagerSelector: V1LabelSelector?, taskmanagerSelector: V1LabelSelector?) = V1Affinity()
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

    private fun createPersistentVolumeClaimSpec(storageConfig: StorageConfig) = V1PersistentVolumeClaimSpec()
        .accessModes(listOf("ReadWriteOnce"))
        .storageClassName(storageConfig.storageClass)
        .resources(
            V1ResourceRequirements()
                .requests(
                    mapOf("storage" to Quantity(storageConfig.size.toString()))
                )
        )

    private fun createObjectMeta(name: String, jobmanagerLabels: Map<String, String>) = V1ObjectMeta()
        .name(name)
        .labels(jobmanagerLabels)

    private fun createVolumeMount(name: String) = V1VolumeMount()
        .mountPath("/var/tmp/data")
        .subPath(name)
        .name(name)

    private fun createEnvVarFromField(name: String, fieldPath: String) = V1EnvVar()
        .name(name)
        .valueFrom(
            V1EnvVarSource()
                .fieldRef(
                    V1ObjectFieldSelector().fieldPath(fieldPath)
                )
        )

    private fun createEnvVar(name: String, value: String) = V1EnvVar()
        .name(name)
        .value(value)

    private fun createResourceRequirements(resourcesConfig: ResourcesConfig) = V1ResourceRequirements()
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

    private fun createSidecarResourceRequirements() = V1ResourceRequirements()
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

    private fun createServicePort(port: Int, name: String) = V1ServicePort()
        .protocol("TCP")
        .port(port)
        .targetPort(IntOrString(name))
        .name(name)

    private fun createContainerPort(port: Int, name: String) = V1ContainerPort()
        .protocol("TCP")
        .containerPort(port)
        .name(name)
}