package com.nextbreakpoint.operator

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

        if (jobmanagerServiceStatus != ResourceStatus.VALID) {
            return true
        }

        if (sidecarDeploymentStatus != ResourceStatus.VALID) {
            return true
        }

        if (jobmanagerStatefulSetStatus != ResourceStatus.VALID) {
            return true
        }

        if (taskmanagerStatefulSetStatus != ResourceStatus.VALID) {
            return true
        }

        if (jobmanagerPersistentVolumeClaimStatus != ResourceStatus.VALID) {
            return true
        }

        if (taskmanagerPersistentVolumeClaimStatus != ResourceStatus.VALID) {
            return true
        }

        return false
    }

    private fun evaluateSidecarDeploymentStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val sidecarDeployment = actualClusterResources.sidecarDeployment ?: return ResourceStatus.MISSING

//        val diff = mutableListOf<String>()

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

        val container = sidecarDeployment.spec.template.spec.containers.get(0)

        if (container.image != targetClusterConfig.sidecar.image) {
            return ResourceStatus.DIVERGENT
        }

        if (container.imagePullPolicy != targetClusterConfig.sidecar.pullPolicy) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarDeployment.spec.template.spec.serviceAccount != targetClusterConfig.sidecar.serviceAccount) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarDeployment.spec.template.spec.imagePullSecrets[0]?.name != targetClusterConfig.sidecar.pullSecrets) {
            return ResourceStatus.DIVERGENT
        }

        if (container.args[0] != "sidecar") {
            return ResourceStatus.DIVERGENT
        }

        if (container.args[1] != "submit" && container.args[1] != "watch") {
            return ResourceStatus.DIVERGENT
        }

        val sidecarNamespace = extractArgument(container.args, "--namespace")

        val sidecarEnvironment = extractArgument(container.args, "--environment")

        val sidecarClusterName = extractArgument(container.args, "--cluster-name")

        val sidecarJarPath = extractArgument(container.args, "--jar-path")

        val sidecarClassName = extractArgument(container.args, "--class-name")

        val sidecarSavepoint = extractArgument(container.args, "--savepoint")

        val sidecarParallelism = extractArgument(container.args, "--parallelism")

        if (sidecarNamespace == null || sidecarNamespace != targetClusterConfig.descriptor.namespace) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarEnvironment == null || sidecarEnvironment != targetClusterConfig.descriptor.environment) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarClusterName == null || sidecarClusterName != targetClusterConfig.descriptor.name) {
            return ResourceStatus.DIVERGENT
        }

        if (container.args.get(1) == "submit" && sidecarJarPath == null) {
            return ResourceStatus.DIVERGENT
        }

        val sidecarArguments = container.args.filter { it.startsWith("--argument") }.map { it.substringAfter("=") }.toList()

        if (sidecarClassName != targetClusterConfig.sidecar.className) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarSavepoint != targetClusterConfig.sidecar.savepoint) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarParallelism == null || sidecarParallelism.toInt() != targetClusterConfig.sidecar.parallelism) {
            return ResourceStatus.DIVERGENT
        }

        if (sidecarJarPath != targetClusterConfig.sidecar.jarPath) {
            return ResourceStatus.DIVERGENT
        }

        val arguments = if (sidecarArguments.isNotEmpty()) sidecarArguments.joinToString(" ") else null

        if (arguments != targetClusterConfig.sidecar.arguments) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun extractArgument(containerArguments: List<String>, name: String) =
        containerArguments.filter { it.startsWith(name) }.map { it.substringAfter("=") }.firstOrNull()

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

        if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets[0]?.name != targetClusterConfig.jobmanager.pullSecrets) {
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

        if (jobmanagerEnvironmentVariables != targetClusterConfig.jobmanager.environmentVariables) {
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

        if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets[0]?.name != targetClusterConfig.taskmanager.pullSecrets) {
            return ResourceStatus.DIVERGENT
        }

        if (taskmanagerStatefulSet.spec.replicas != targetClusterConfig.taskmanager.replicas) {
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

        if (jobmanagerPersistentVolumeClaim.spec.storageClassName != targetClusterConfig.jobmanager.storage.storageClass) {
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

        if (taskmanagerPersistentVolumeClaim.spec.storageClassName != targetClusterConfig.taskmanager.storage.storageClass) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }
}