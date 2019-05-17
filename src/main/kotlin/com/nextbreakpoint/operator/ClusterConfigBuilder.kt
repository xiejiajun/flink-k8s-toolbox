package com.nextbreakpoint.operator

import com.nextbreakpoint.model.*
import com.nextbreakpoint.operator.model.*
import io.kubernetes.client.models.V1ObjectMeta

class ClusterConfigBuilder(
    private val metadata: V1ObjectMeta,
    private val spec: V1FlinkClusterSpec
) {
    fun build() = ClusterConfig(
        descriptor = ClusterDescriptor(
            namespace = metadata.namespace,
            name = metadata.name,
            environment = spec.environment ?: "test"
        ),
        jobmanager = JobManagerConfig(
            image = spec.flinkImage,
            pullSecrets = spec.pullSecrets,
            pullPolicy = spec.pullPolicy ?: "Always",
            serviceMode = spec.serviceMode ?: "NodePort",
            serviceAccount = spec.jobmanagerServiceAccount ?: "default",
            environmentVariables = spec.jobmanagerEnvironmentVariables?.map {
                EnvironmentVariable(
                    it.name,
                    it.value
                )
            }?.toList() ?: listOf(),
            resources = ResourcesConfig(
                cpus = spec.jobmanagerCpus ?: 1f,
                memory = spec.jobmanagerMemory ?: 512
            ),
            storage = StorageConfig(
                storageClass = spec.jobmanagerStorageClass ?: "standard",
                size = spec.jobmanagerStorageSize ?: 2
            )
        ),
        taskmanager = TaskManagerConfig(
            image = spec.flinkImage,
            pullSecrets = spec.pullSecrets,
            pullPolicy = spec.pullPolicy ?: "Always",
            serviceAccount = spec.taskmanagerServiceAccount ?: "default",
            environmentVariables = spec.taskmanagerEnvironmentVariables?.map {
                EnvironmentVariable(
                    it.name,
                    it.value
                )
            }?.toList() ?: listOf(),
            replicas = spec.taskmanagerReplicas ?: 1,
            taskSlots = spec.taskmanagerTaskSlots ?: 1,
            resources = ResourcesConfig(
                cpus = spec.taskmanagerCpus ?: 1f,
                memory = spec.taskmanagerMemory ?: 1024
            ),
            storage = StorageConfig(
                storageClass = spec.taskmanagerStorageClass ?: "standard",
                size = spec.taskmanagerStorageSize ?: 2
            )
        ),
        sidecar = SidecarConfig(
            image = spec.sidecarImage,
            pullSecrets = spec.pullSecrets,
            pullPolicy = spec.pullPolicy ?: "Always",
            serviceAccount = spec.sidecarServiceAccount ?: "default",
            className = spec.sidecarClassName,
            jarPath = spec.sidecarJarPath,
            savepoint = spec.sidecarSavepoint,
            arguments = spec.sidecarArguments?.joinToString(" "),
            parallelism = spec.sidecarParallelism ?: 1
        )
    )
}