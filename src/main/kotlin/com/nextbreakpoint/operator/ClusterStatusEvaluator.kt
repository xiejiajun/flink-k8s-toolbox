package com.nextbreakpoint.operator

import com.nextbreakpoint.command.RunOperator
import com.nextbreakpoint.operator.model.*

class ClusterStatusEvaluator {
    fun hasDiverged(
        targetClusterConfig: ClusterConfig,
        resources: ClusterResources
    ) : Boolean {
        val jobmanagerServiceStatus = evaluateJobManagerServiceStatus(resources, targetClusterConfig)

        val sidecarDeploymentStatus = evaluateSidecarDeploymentStatus(resources, targetClusterConfig)

        val jobmanagerStatefulSetStatus = evaluateJobManagerStatefulSetStatus(resources, targetClusterConfig)

        val taskmanagerStatefulSetStatus = evaluateTaskManagerStatefulSetStatus(resources, targetClusterConfig)

        val jobmanagerPersistentVolumeClaimStatus = evaluateJobManagerPersistentVolumeClaimStatus(resources, targetClusterConfig)

        val taskmanagerPersistentVolumeClaimStatus = evaluateTaskManagerPersistentVolumeClaimStatus(resources, targetClusterConfig)

//        val clusterConfig = ClusterConfig(
//            descriptor = ClusterDescriptor(
//                namespace = targetClusterConfig.descriptor.namespace,
//                name = targetClusterConfig.descriptor.name,
//                environment = environment
//            ),
//            jobmanager = JobManagerConfig(
//                image = jobmanagerImage,
//                pullSecrets = pullSecrets,
//                pullPolicy = jobmanagerPullPolicy,
//                serviceMode = serviceMode,
//                serviceAccount = jobmanagerServiceAccount,
//                environmentVariables = jobmanagerEnvironmentVariables,
//                resources = ResourcesConfig(
//                    cpus = jobmanagerCpu,
//                    memory = jobmanagerMemory
//                ),
//                storage = StorageConfig(
//                    storageClass = jobmanagerStorageClassName,
//                    size = jobmanagerStorageSize
//                )
//            ),
//            taskmanager = TaskManagerConfig(
//                image = taskmanagerImage,
//                pullSecrets = pullSecrets,
//                pullPolicy = taskmanagerPullPolicy,
//                serviceAccount = taskmanagerServiceAccount,
//                environmentVariables = taskmanagerEnvironmentVariables,
//                replicas = taskmanagerReplicas,
//                taskSlots = taskmanagerTaskSlots,
//                resources = ResourcesConfig(
//                    cpus = taskmanagerCpu,
//                    memory = taskmanagerMemory
//                ),
//                storage = StorageConfig(
//                    storageClass = taskmanagerStorageClassName,
//                    size = taskmanagerStorageSize
//                )
//            ),
//            sidecar = SidecarConfig(
//                image = sidecarImage,
//                pullSecrets = pullSecrets,
//                pullPolicy = sidecarPullPolicy,
//                serviceAccount = sidecarServiceAccount,
//                className = sidecarClassName,
//                jarPath = sidecarJarPath,
//                savepoint = sidecarSavepoint,
//                arguments = sidecarArguments.joinToString(" "),
//                parallelism = sidecarParallelism?.toInt() ?: 1
//            )
//        )
//
//        val diverged = clusterConfig.equals(targetClusterConfig).not()
//
//        if (diverged) {
//            RunOperator.logger.info("Current config: $clusterConfig")
//            RunOperator.logger.info("Desired config: $targetClusterConfig")
//        }

        return false
    }

    private fun evaluateSidecarDeploymentStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val sidecarDeployment = actualClusterResources.sidecarDeployment ?: return ResourceStatus.MISSING

        if (sidecarDeployment.metadata.labels["component"]?.equals("flink") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarDeployment.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarDeployment.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarDeployment.spec.template.spec.containers.size != 1) {
            return ResourceStatus.DIVERGENT
        }

        val sidecarImage = sidecarDeployment.spec.template.spec.containers.get(0).image

        val sidecarPullPolicy = sidecarDeployment.spec.template.spec.containers.get(0).imagePullPolicy

        val sidecarServiceAccount = sidecarDeployment.spec.template.spec.serviceAccount




        val containerArguments = sidecarDeployment.spec.template.spec.containers.get(0).args

        if (containerArguments.get(0) != "sidecar") {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
        }

        if (containerArguments.get(1) != "submit" && containerArguments.get(1) != "watch") {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
        }

        val sidecarNamespace =
            containerArguments.filter { it.startsWith("--namespace") }.map { it.substringAfter("=") }.firstOrNull()

        val sidecarEnvironment =
            containerArguments.filter { it.startsWith("--environment") }.map { it.substringAfter("=") }.firstOrNull()

        val sidecarClusterName =
            containerArguments.filter { it.startsWith("--cluster-name") }.map { it.substringAfter("=") }.firstOrNull()

        val sidecarJarPath =
            containerArguments.filter { it.startsWith("--jar-path") }.map { it.substringAfter("=") }.firstOrNull()

        val sidecarClassName =
            containerArguments.filter { it.startsWith("--class-name") }.map { it.substringAfter("=") }.firstOrNull()

        val sidecarSavepoint =
            containerArguments.filter { it.startsWith("--savepoint") }.map { it.substringAfter("=") }.firstOrNull()

        val sidecarParallelism =
            containerArguments.filter { it.startsWith("--parallelism") }.map { it.substringAfter("=") }.firstOrNull()

        if (sidecarNamespace == null || sidecarNamespace != targetClusterConfig.descriptor.namespace) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
        }

        if (sidecarEnvironment == null || sidecarEnvironment != targetClusterConfig.descriptor.environment) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
        }

        if (sidecarClusterName == null || sidecarClusterName != targetClusterConfig.descriptor.name) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
        }

        if (containerArguments.get(1) == "submit" && sidecarJarPath == null) {
            RunOperator.logger.warn("Sidecar argument are: ${containerArguments.joinToString(" ")}}")
        }

        val sidecarArguments =
            containerArguments.filter { it.startsWith("--argument") }.map { it.substringAfter("=") }.toList()

        val pullSecrets =
            if (sidecarDeployment.spec.template.spec.imagePullSecrets?.isNotEmpty() == true) sidecarDeployment.spec.template.spec.imagePullSecrets.get(0).name else null

        return ResourceStatus.VALID
    }

    private fun evaluateJobManagerServiceStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val jobmanagerService = actualClusterResources.jobmanagerService ?: return ResourceStatus.MISSING

        if (jobmanagerService.metadata.labels["role"]?.equals("jobmanager") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerService.metadata.labels["component"]?.equals("flink") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerService.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerService.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerService.spec.type != targetClusterConfig.jobmanager.serviceMode) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateJobManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val jobmanagerStatefulSet = actualClusterResources.jobmanagerStatefulSet ?: return ResourceStatus.MISSING

        if (jobmanagerStatefulSet.metadata.labels["role"]?.equals("jobmanager") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            return ResourceStatus.DIVERGENT
        }

        val container = jobmanagerStatefulSet.spec.template.spec.containers.get(0)

        if (container.image != targetClusterConfig.jobmanager.image) {
            return ResourceStatus.DIVERGENT
        }

        if (container.imagePullPolicy != targetClusterConfig.jobmanager.pullPolicy) {
            return ResourceStatus.DIVERGENT
        }

        if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(targetClusterConfig.jobmanager.resources.cpus) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.spec.template.spec.serviceAccount != targetClusterConfig.jobmanager.serviceAccount) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets.get(0)?.name != targetClusterConfig.jobmanager.pullSecrets) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            return ResourceStatus.DIVERGENT
        }

        val persistentVolumeClaim = jobmanagerStatefulSet.spec.volumeClaimTemplates.get(0)

        if (persistentVolumeClaim.spec.storageClassName != targetClusterConfig.jobmanager.storage.storageClass) {
            return ResourceStatus.DIVERGENT
        }

        if (persistentVolumeClaim.spec.resources.requests.get("storage")?.number?.toInt()?.equals(targetClusterConfig.jobmanager.storage.size) != true) {
            return ResourceStatus.DIVERGENT
        }

        val jobmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val jobmanagerEnvironmentEnvVar = container.env.filter { it.name == "FLINK_ENVIRONMENT" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val jobmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val jobmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val jobmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        if (jobmanagerMemoryEnvVar.value.toInt() < targetClusterConfig.jobmanager.resources.memory) {
            return ResourceStatus.DIVERGENT
        }

        val jobmanagerEnvironmentVariables = jobmanagerStatefulSet.spec.template.spec.containers.get(0).env
            .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
            .filter { it.name != "FLINK_JM_HEAP" }
            .filter { it.name != "FLINK_ENVIRONMENT" }
            .filter { it.name != "POD_NAMESPACE" }
            .filter { it.name != "POD_NAME" }
            .map { EnvironmentVariable(it.name, it.value) }
            .toList()

        if (!jobmanagerEnvironmentVariables.equals(targetClusterConfig.jobmanager.environmentVariables)) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateTaskManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val taskmanagerStatefulSet = actualClusterResources.taskmanagerStatefulSet ?: return ResourceStatus.MISSING

        if (taskmanagerStatefulSet.metadata.labels["role"]?.equals("taskmanager") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            return ResourceStatus.DIVERGENT
        }

        val container = taskmanagerStatefulSet.spec.template.spec.containers.get(0)

        if (container.image != targetClusterConfig.taskmanager.image) {
            return ResourceStatus.DIVERGENT
        }

        if (container.imagePullPolicy != targetClusterConfig.taskmanager.pullPolicy) {
            return ResourceStatus.DIVERGENT
        }

        if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(targetClusterConfig.taskmanager.resources.cpus) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.spec.template.spec.serviceAccount != targetClusterConfig.taskmanager.serviceAccount) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets.get(0)?.name != targetClusterConfig.taskmanager.pullSecrets) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            return ResourceStatus.DIVERGENT
        }

        val persistentVolumeClaim = taskmanagerStatefulSet.spec.volumeClaimTemplates.get(0)

        if (persistentVolumeClaim.spec.storageClassName != targetClusterConfig.taskmanager.storage.storageClass) {
            return ResourceStatus.DIVERGENT
        }

        if (persistentVolumeClaim.spec.resources.requests.get("storage")?.number?.toInt()?.equals(targetClusterConfig.taskmanager.storage.size) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.spec.replicas != targetClusterConfig.taskmanager.replicas) {
            return ResourceStatus.DIVERGENT
        }

        val taskmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val taskmanagerEnvironmentEnvVar = container.env.filter { it.name == "FLINK_ENVIRONMENT" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val taskmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val taskmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val taskmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        val taskmanagerTaskSlotsEnvVar = container.env.filter { it.name == "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }.firstOrNull() ?: return ResourceStatus.DIVERGENT

        if (taskmanagerMemoryEnvVar.value.toInt() < targetClusterConfig.taskmanager.resources.memory) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerTaskSlotsEnvVar.value.toInt() != targetClusterConfig.taskmanager.taskSlots) {
            return ResourceStatus.DIVERGENT
        }

        val taskmanagerEnvironmentVariables = container.env
            .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
            .filter { it.name != "FLINK_TM_HEAP" }
            .filter { it.name != "FLINK_ENVIRONMENT" }
            .filter { it.name != "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }
            .filter { it.name != "POD_NAMESPACE" }
            .filter { it.name != "POD_NAME" }
            .map { EnvironmentVariable(it.name, it.value) }
            .toList()

        if (!taskmanagerEnvironmentVariables.equals(targetClusterConfig.taskmanager.environmentVariables)) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateJobManagerPersistentVolumeClaimStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val jobmanagerPersistentVolumeClaim = actualClusterResources.jobmanagerPersistentVolumeClaim ?: return ResourceStatus.MISSING

        if (jobmanagerPersistentVolumeClaim.metadata.labels["role"]?.equals("jobmanager") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["component"]?.equals("flink") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (!jobmanagerPersistentVolumeClaim.spec.storageClassName.equals(targetClusterConfig.jobmanager.storage.storageClass)) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateTaskManagerPersistentVolumeClaimStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val taskmanagerPersistentVolumeClaim = actualClusterResources.taskmanagerPersistentVolumeClaim ?: return ResourceStatus.MISSING

        if (taskmanagerPersistentVolumeClaim.metadata.labels["role"]?.equals("taskmanager") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["component"]?.equals("flink") != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            return ResourceStatus.DIVERGENT
        }

        if (!taskmanagerPersistentVolumeClaim.spec.storageClassName.equals(targetClusterConfig.taskmanager.storage.storageClass)) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }
}