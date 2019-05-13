package com.nextbreakpoint.operator

import com.nextbreakpoint.command.RunOperator
import com.nextbreakpoint.model.*
import com.nextbreakpoint.operator.model.*

class ClusterResourcesDiffEvaluator {
    fun hasDiverged(
        targetClusterConfig: ClusterConfig,
        resources: ClusterResources
    ) : Boolean {
        val service = resources.jobmanagerService
        val deployment = resources.sidecarDeployment
        val jobmanagerStatefulSet = resources.jobmanagerStatefulSet
        val taskmanagerStatefulSet = resources.taskmanagerStatefulSet
        val jobmanagerPersistentVolumeClaim = resources.jobmanagerPersistentVolumeClaim
        val taskmanagerPersistentVolumeClaim = resources.taskmanagerPersistentVolumeClaim

        if (service == null) {
            return true
        }

        if (deployment == null) {
            return true
        }

        if (jobmanagerStatefulSet == null) {
            return true
        }

        if (taskmanagerStatefulSet == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim == null) {
            return true
        }

        if (deployment.spec.template.spec.containers.size != 1) {
            return true
        }

        if (deployment.metadata.labels.get("cluster") == null) {
            return true
        }

        if (deployment.metadata.labels.get("component") == null) {
            return true
        }

        if (deployment.metadata.labels.get("environment") == null) {
            return true
        }

        if (service.metadata.labels.get("cluster") == null) {
            return true
        }

        if (service.metadata.labels.get("role") == null) {
            return true
        }

        if (service.metadata.labels.get("component") == null) {
            return true
        }

        if (service.metadata.labels.get("environment") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("cluster") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("role") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("component") == null) {
            return true
        }

        if (jobmanagerStatefulSet.metadata.labels.get("environment") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("cluster") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("role") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("component") == null) {
            return true
        }

        if (taskmanagerStatefulSet.metadata.labels.get("environment") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("cluster") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("role") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("component") == null) {
            return true
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels.get("environment") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("cluster") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("role") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("component") == null) {
            return true
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels.get("environment") == null) {
            return true
        }

        val sidecarImage = deployment.spec.template.spec.containers.get(0).image
        val sidecarPullPolicy = deployment.spec.template.spec.containers.get(0).imagePullPolicy
        val sidecarServiceAccount = deployment.spec.template.spec.serviceAccount

        val containerArguments = deployment.spec.template.spec.containers.get(0).args

        if (containerArguments.get(0) != "sidecar") {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (containerArguments.get(1) != "submit" && containerArguments.get(1) != "watch") {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        val sidecarNamespace = containerArguments.filter{ it.startsWith("--namespace") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarEnvironment = containerArguments.filter{ it.startsWith("--environment") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarClusterName = containerArguments.filter{ it.startsWith("--cluster-name") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarJarPath = containerArguments.filter{ it.startsWith("--jar-path") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarClassName = containerArguments.filter{ it.startsWith("--class-name") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarSavepoint = containerArguments.filter{ it.startsWith("--savepoint") }.map { it.substringAfter("=") }.firstOrNull()
        val sidecarParallelism = containerArguments.filter{ it.startsWith("--parallelism") }.map { it.substringAfter("=") }.firstOrNull()

        if (sidecarNamespace == null || sidecarNamespace != targetClusterConfig.descriptor.namespace) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (sidecarEnvironment == null || sidecarEnvironment != targetClusterConfig.descriptor.environment) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (sidecarClusterName == null || sidecarClusterName != targetClusterConfig.descriptor.name) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        if (containerArguments.get(1) == "submit" && sidecarJarPath == null) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
            return true
        }

        val sidecarArguments = containerArguments.filter{ it.startsWith("--argument") }.map { it.substringAfter("=") }.toList()

        val pullSecrets = if (deployment.spec.template.spec.imagePullSecrets != null && !deployment.spec.template.spec.imagePullSecrets.isEmpty()) deployment.spec.template.spec.imagePullSecrets.get(0).name else null

        if (jobmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            return true
        }

        val jobmanagerImage = jobmanagerStatefulSet.spec.template.spec.containers.get(0).image
        val jobmanagerPullPolicy = jobmanagerStatefulSet.spec.template.spec.containers.get(0).imagePullPolicy
        val jobmanagerServiceAccount = jobmanagerStatefulSet.spec.template.spec.serviceAccount

        val jobmanagerCpuQuantity = jobmanagerStatefulSet.spec.template.spec.containers.get(0).resources.limits.get("cpu")

        if (jobmanagerCpuQuantity == null) {
            return true
        }

        val jobmanagerCpu = jobmanagerCpuQuantity.number.toFloat()

        if (jobmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            return true
        }

        val jobmanagerStorageClassName = jobmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.storageClassName
        val jobmanagerStorageSizeQuantity = jobmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.resources.requests.get("storage")

        if (jobmanagerStorageSizeQuantity == null) {
            return true
        }

        val jobmanagerStorageSize = jobmanagerStorageSizeQuantity.number.toInt()

        if (taskmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            return true
        }

        val taskmanagerImage = taskmanagerStatefulSet.spec.template.spec.containers.get(0).image
        val taskmanagerPullPolicy = taskmanagerStatefulSet.spec.template.spec.containers.get(0).imagePullPolicy
        val taskmanagerServiceAccount = taskmanagerStatefulSet.spec.template.spec.serviceAccount

        val taskmanagerCpuQuantity = taskmanagerStatefulSet.spec.template.spec.containers.get(0).resources.limits.get("cpu")

        if (taskmanagerCpuQuantity == null) {
            return true
        }

        val taskmanagerCpu = taskmanagerCpuQuantity.number.toFloat()

        if (taskmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            return true
        }

        val taskmanagerStorageClassName = taskmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.storageClassName
        val taskmanagerStorageSizeQuantity = taskmanagerStatefulSet.spec.volumeClaimTemplates.get(0).spec.resources.requests.get("storage")

        if (taskmanagerStorageSizeQuantity == null) {
            return true
        }

        val taskmanagerStorageSize = taskmanagerStorageSizeQuantity.number.toInt()

        val taskmanagerReplicas = taskmanagerStatefulSet.spec.replicas

        val environment = jobmanagerStatefulSet.metadata.labels.get("environment").orEmpty()

        val jobmanagerMemoryEnvVar = jobmanagerStatefulSet.spec.template.spec.containers.get(0).env.filter { it.name == "FLINK_JM_HEAP" }.firstOrNull()

        if (jobmanagerMemoryEnvVar == null) {
            return true
        }

        val taskmanagerMemoryEnvVar = taskmanagerStatefulSet.spec.template.spec.containers.get(0).env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull()

        if (taskmanagerMemoryEnvVar == null) {
            return true
        }

        val taskmanagerTaskSlotsEnvVar = taskmanagerStatefulSet.spec.template.spec.containers.get(0).env.filter { it.name == "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }.firstOrNull()

        if (taskmanagerTaskSlotsEnvVar == null) {
            return true
        }

        val jobmanagerEnvironmentVariables = jobmanagerStatefulSet.spec.template.spec.containers.get(0).env
            .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
            .filter { it.name != "FLINK_JM_HEAP" }
            .filter { it.name != "FLINK_ENVIRONMENT" }
            .filter { it.name != "POD_NAMESPACE" }
            .filter { it.name != "POD_NAME" }
            .map { EnvironmentVariable(it.name, it.value) }
            .toList()

        val taskmanagerEnvironmentVariables = taskmanagerStatefulSet.spec.template.spec.containers.get(0).env
            .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
            .filter { it.name != "FLINK_TM_HEAP" }
            .filter { it.name != "FLINK_ENVIRONMENT" }
            .filter { it.name != "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }
            .filter { it.name != "POD_NAMESPACE" }
            .filter { it.name != "POD_NAME" }
            .map { EnvironmentVariable(it.name, it.value) }
            .toList()

        val jobmanagerMemory = jobmanagerMemoryEnvVar.value.toInt()

        val taskmanagerMemory = taskmanagerMemoryEnvVar.value.toInt()

        val taskmanagerTaskSlots = taskmanagerTaskSlotsEnvVar.value.toInt()

        val serviceMode = service.spec.type

        val clusterConfig = ClusterConfig(
            descriptor = ClusterDescriptor(
                namespace = targetClusterConfig.descriptor.namespace,
                name = targetClusterConfig.descriptor.name,
                environment = environment
            ),
            jobmanager = JobManagerConfig(
                image = jobmanagerImage,
                pullSecrets = pullSecrets,
                pullPolicy = jobmanagerPullPolicy,
                serviceMode = serviceMode,
                serviceAccount = jobmanagerServiceAccount,
                environmentVariables = jobmanagerEnvironmentVariables,
                resources = ResourcesConfig(
                    cpus = jobmanagerCpu,
                    memory = jobmanagerMemory
                ),
                storage = StorageConfig(
                    storageClass = jobmanagerStorageClassName,
                    size = jobmanagerStorageSize
                )
            ),
            taskmanager = TaskManagerConfig(
                image = taskmanagerImage,
                pullSecrets = pullSecrets,
                pullPolicy = taskmanagerPullPolicy,
                serviceAccount = taskmanagerServiceAccount,
                environmentVariables = taskmanagerEnvironmentVariables,
                replicas = taskmanagerReplicas,
                taskSlots = taskmanagerTaskSlots,
                resources = ResourcesConfig(
                    cpus = taskmanagerCpu,
                    memory = taskmanagerMemory
                ),
                storage = StorageConfig(
                    storageClass = taskmanagerStorageClassName,
                    size = taskmanagerStorageSize
                )
            ),
            sidecar = SidecarConfig(
                image = sidecarImage,
                pullSecrets = pullSecrets,
                pullPolicy = sidecarPullPolicy,
                serviceAccount = sidecarServiceAccount,
                className = sidecarClassName,
                jarPath = sidecarJarPath,
                savepoint = sidecarSavepoint,
                arguments = sidecarArguments.joinToString(" "),
                parallelism = sidecarParallelism?.toInt() ?: 1
            )
        )

        val diverged = clusterConfig.equals(targetClusterConfig).not()

        if (diverged) {
            RunOperator.logger.info("Current config: $clusterConfig")
            RunOperator.logger.info("Desired config: $targetClusterConfig")
        }

        return diverged
    }
}