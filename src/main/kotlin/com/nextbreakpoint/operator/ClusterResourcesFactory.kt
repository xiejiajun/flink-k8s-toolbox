package com.nextbreakpoint.operator

import com.nextbreakpoint.operator.model.*
import io.kubernetes.client.custom.IntOrString
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.*

object ClusterResourcesFactory {
    fun createJobManagerService(
        srvPort8081: V1ServicePort,
        srvPort6123: V1ServicePort,
        srvPort6124: V1ServicePort,
        srvPort6125: V1ServicePort,
        jobmanagerLabels: Map<String, String>,
        clusterName: String?,
        serviceMode: String?
    ): V1Service? {
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
            .type(serviceMode)

        val jobmanagerServiceMetadata =
            createObjectMeta("flink-jobmanager-$clusterName", jobmanagerLabels)

        val jobmanagerService = V1Service().spec(jobmanagerServiceSpec).metadata(jobmanagerServiceMetadata)

        return jobmanagerService
    }

    fun createSidecarDeployment(
        podNameEnvVar: V1EnvVar,
        podNamespaceEnvVar: V1EnvVar,
        environmentEnvVar: V1EnvVar,
        jobmanagerAffinity: V1Affinity,
        sidecarLabels: Map<String, String>,
        sidecarSelector: V1LabelSelector?,
        namespace: String?,
        environment: String?,
        clusterName: String?,
        image: String?,
        pullPolicy: String?,
        pullSecrets: String?,
        serviceAccount: String?,
        className: String?,
        jarPath: String?,
        jobArguments: String?,
        savepoint: String?,
        parallelism: Int
    ): V1Deployment? {
        val arguments = mutableListOf<String>()

        if (jarPath != null) {
            arguments.addAll(
                listOf(
                    "sidecar",
                    "submit",
                    "--namespace=$namespace",
                    "--environment=$environment",
                    "--cluster-name=$clusterName",
                    "--jar-path=$jarPath",
                    "--parallelism=$parallelism"
                )
            )

            if (className != null) {
                arguments.add("--class-name=$className")
            }

            if (savepoint != null) {
                arguments.add("--savepoint=$savepoint")
            }

            jobArguments?.split(" ")?.forEach { argument ->
                arguments.add("--argument=$argument")
            }
        } else {
            arguments.addAll(
                listOf(
                    "sidecar",
                    "watch",
                    "--namespace=$namespace",
                    "--environment=$environment",
                    "--cluster-name=$clusterName"
                )
            )
        }

        val sidecar = V1Container()
            .image(image)
            .imagePullPolicy(pullPolicy)
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

        val sidecarPullSecrets = if (pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(pullSecrets)
            )
        } else null

        val sidecarPodSpec = V1PodSpec()
            .containers(
                listOf(sidecar)
            )
            .serviceAccountName(serviceAccount)
            .imagePullSecrets(sidecarPullSecrets)
            .affinity(jobmanagerAffinity)

        val sidecarMetadata =
            createObjectMeta("flink-sidecar-$clusterName", sidecarLabels)

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

    fun createJobManagerStatefulSet(
        podNameEnvVar: V1EnvVar,
        podNamespaceEnvVar: V1EnvVar,
        environmentEnvVar: V1EnvVar,
        rpcAddressEnvVar: V1EnvVar,
        jobManagerHeapEnvVar: V1EnvVar,
        port8081: V1ContainerPort,
        port6123: V1ContainerPort,
        port6124: V1ContainerPort,
        port6125: V1ContainerPort,
        jobmanagerVolumeMount: V1VolumeMount,
        jobmanagerResourceRequirements: V1ResourceRequirements,
        jobmanagerAffinity: V1Affinity,
        jobmanagerSelector: V1LabelSelector?,
        jobmanagerLabels: Map<String, String>,
        updateStrategy: V1StatefulSetUpdateStrategy?,
        environmentVariables: List<EnvironmentVariable>?,
        clusterName: String?,
        image: String?,
        pullPolicy: String?,
        pullSecrets: String?,
        serviceAccount: String?,
        storageConfig: StorageConfig
    ): V1StatefulSet {
        val jobmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            rpcAddressEnvVar,
            jobManagerHeapEnvVar
        )

        val jobmanagerUserVariables = environmentVariables?.map { createEnvVar(it.name, it.value) }?.toList().orEmpty()

        jobmanagerVariables.addAll(jobmanagerUserVariables)

        val jobmanager = V1Container()
            .image(image)
            .imagePullPolicy(pullPolicy)
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

        val jobmanagerPullSecrets = if (pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(pullSecrets)
            )
        } else null

        val jobmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(jobmanager)
            )
            .serviceAccountName(serviceAccount)
            .imagePullSecrets(jobmanagerPullSecrets)
            .affinity(jobmanagerAffinity)

        val jobmanagerMetadata =
            createObjectMeta("flink-jobmanager-$clusterName", jobmanagerLabels)

        val jobmanagerVolumeClaim =
            createPersistentVolumeClaimSpec(storageConfig)

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

    fun createTaskManagerStatefulSet(
        podNameEnvVar: V1EnvVar,
        podNamespaceEnvVar: V1EnvVar,
        environmentEnvVar: V1EnvVar,
        rpcAddressEnvVar: V1EnvVar,
        taskManagerHeapEnvVar: V1EnvVar,
        numberOfTaskSlotsEnvVar: V1EnvVar,
        port6121: V1ContainerPort,
        port6122: V1ContainerPort,
        taskmanagerVolumeMount: V1VolumeMount,
        taskmanagerResourceRequirements: V1ResourceRequirements,
        jobmanagerSelector: V1LabelSelector?,
        taskmanagerSelector: V1LabelSelector?,
        taskmanagerLabels: Map<String, String>,
        updateStrategy: V1StatefulSetUpdateStrategy?,
        environmentVariables: List<EnvironmentVariable>?,
        clusterName: String?,
        replicas: Int,
        image: String?,
        pullPolicy: String?,
        pullSecrets: String?,
        serviceAccount: String?,
        storageConfig: StorageConfig
    ): V1StatefulSet? {
        val taskmanagerVariables = mutableListOf(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            rpcAddressEnvVar,
            taskManagerHeapEnvVar,
            numberOfTaskSlotsEnvVar
        )

        val taskmanagerUserVariables = environmentVariables?.map { createEnvVar(it.name, it.value) }?.toList().orEmpty()

        taskmanagerVariables.addAll(taskmanagerUserVariables)

        val taskmanager = V1Container()
            .image(image)
            .imagePullPolicy(pullPolicy)
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

        val taskmanagerPullSecrets = if (pullSecrets != null) {
            listOf(
                V1LocalObjectReference().name(pullSecrets)
            )
        } else null

        val taskmanagerPodSpec = V1PodSpec()
            .containers(
                listOf(taskmanager)
            )
            .serviceAccountName(serviceAccount)
            .imagePullSecrets(taskmanagerPullSecrets)
            .affinity(taskmanagerAffinity)

        val taskmanagerMetadata =
            createObjectMeta("flink-taskmanager--$clusterName", taskmanagerLabels)

        val taskmanagerVolumeClaim =
            createPersistentVolumeClaimSpec(storageConfig)

        val taskmanagerStatefulSet = V1StatefulSet()
            .metadata(taskmanagerMetadata)
            .spec(
                V1StatefulSetSpec()
                    .replicas(replicas)
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

    fun createAffinity(jobmanagerSelector: V1LabelSelector?, taskmanagerSelector: V1LabelSelector?): V1Affinity = V1Affinity()
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

    fun createPersistentVolumeClaimSpec(storageConfig: StorageConfig): V1PersistentVolumeClaimSpec = V1PersistentVolumeClaimSpec()
        .accessModes(listOf("ReadWriteOnce"))
        .storageClassName(storageConfig.storageClass)
        .resources(
            V1ResourceRequirements()
                .requests(
                    mapOf("storage" to Quantity(storageConfig.size.toString()))
                )
        )

    fun createObjectMeta(name: String, jobmanagerLabels: Map<String, String>): V1ObjectMeta = V1ObjectMeta()
        .name(name)
        .labels(jobmanagerLabels)

    fun createVolumeMount(name: String): V1VolumeMount = V1VolumeMount()
        .mountPath("/var/tmp/data")
        .subPath(name)
        .name(name)

    fun createEnvVarFromField(name: String, fieldPath: String): V1EnvVar = V1EnvVar()
        .name(name)
        .valueFrom(
            V1EnvVarSource()
                .fieldRef(
                    V1ObjectFieldSelector().fieldPath(fieldPath)
                )
        )

    fun createEnvVar(name: String, value: String): V1EnvVar = V1EnvVar()
        .name(name)
        .value(value)

    fun createResourceRequirements(resourcesConfig: ResourcesConfig): V1ResourceRequirements = V1ResourceRequirements()
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

    fun createSidecarResourceRequirements(): V1ResourceRequirements = V1ResourceRequirements()
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

    fun createServicePort(port: Int, name: String): V1ServicePort = V1ServicePort()
        .protocol("TCP")
        .port(port)
        .targetPort(IntOrString(name))
        .name(name)

    fun createContainerPort(port: Int, name: String): V1ContainerPort = V1ContainerPort()
        .protocol("TCP")
        .containerPort(port)
        .name(name)
}