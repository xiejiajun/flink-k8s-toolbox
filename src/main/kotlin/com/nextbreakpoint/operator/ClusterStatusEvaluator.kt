package com.nextbreakpoint.operator

import com.nextbreakpoint.operator.model.*

class ClusterStatusEvaluator {
    fun status(
        targetCluster: Cluster,
        resources: ClusterResources
    ): ClusterStatus {
        val jobmanagerServiceStatus = evaluateJobManagerServiceStatus(resources, targetCluster)

        val sidecarDeploymentStatus = evaluateSidecarDeploymentStatus(resources, targetCluster)

        val jobmanagerStatefulSetStatus = evaluateJobManagerStatefulSetStatus(resources, targetCluster)

        val taskmanagerStatefulSetStatus = evaluateTaskManagerStatefulSetStatus(resources, targetCluster)

        val jobmanagerPersistentVolumeClaimStatus = evaluateJobManagerPersistentVolumeClaimStatus(resources, targetCluster)

        val taskmanagerPersistentVolumeClaimStatus = evaluateTaskManagerPersistentVolumeClaimStatus(resources, targetCluster)

        return ClusterStatus(
            jobmanagerService = jobmanagerServiceStatus,
            sidecarDeployment = sidecarDeploymentStatus,
            jobmanagerStatefulSet = jobmanagerStatefulSetStatus,
            taskmanagerStatefulSet = taskmanagerStatefulSetStatus,
            jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaimStatus,
            taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaimStatus
        )
    }

    private fun evaluateSidecarDeploymentStatus(
        actualClusterResources: ClusterResources,
        targetCluster: Cluster
    ): Pair<ResourceStatus, List<String>> {
        val sidecarDeployment = actualClusterResources.sidecarDeployment ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (sidecarDeployment.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (sidecarDeployment.metadata.labels["cluster"]?.equals(targetCluster.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (sidecarDeployment.metadata.labels["environment"]?.equals(targetCluster.descriptor.environment) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (sidecarDeployment.spec.template.spec.serviceAccountName != targetCluster.sidecar.serviceAccount) {
            statusReport.add("service account does not match")
        }

        if (sidecarDeployment.spec.template.spec.imagePullSecrets.size != 1) {
            statusReport.add("unexpected number of pull secrets")
        } else {
            if (sidecarDeployment.spec.template.spec.imagePullSecrets[0].name != targetCluster.sidecar.pullSecrets) {
                statusReport.add("pull secrets don't match")
            }
        }

        if (sidecarDeployment.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        } else {
            val container = sidecarDeployment.spec.template.spec.containers.get(0)

            if (container.image != targetCluster.sidecar.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != targetCluster.sidecar.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.args.size < 1 || container.args[0] != "sidecar") {
                statusReport.add("missing sidecar command: ${container.args.joinToString(separator = ",")}")
            } else {
                if (container.args.size < 2 || (container.args[1] != "submit" && container.args[1] != "watch")) {
                    statusReport.add("unexpected sub command: ${container.args.joinToString(separator = ",")}")
                } else {
                    val sidecarNamespace = extractArgument(container.args, "--namespace")

                    val sidecarEnvironment = extractArgument(container.args, "--environment")

                    val sidecarClusterName = extractArgument(container.args, "--cluster-name")

                    val sidecarParallelism = extractArgument(container.args, "--parallelism")

                    val sidecarJarPath = extractArgument(container.args, "--jar-path")

                    val sidecarClassName = extractArgument(container.args, "--class-name")

                    val sidecarSavepoint = extractArgument(container.args, "--savepoint")

                    if (sidecarNamespace == null || sidecarNamespace != targetCluster.descriptor.namespace) {
                        statusReport.add("unexpected argument namespace: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarEnvironment == null || sidecarEnvironment != targetCluster.descriptor.environment) {
                        statusReport.add("unexpected argument environment: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarClusterName == null || sidecarClusterName != targetCluster.descriptor.name) {
                        statusReport.add("unexpected argument cluster name: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarParallelism == null || sidecarParallelism.toInt() != targetCluster.sidecar.parallelism) {
                        statusReport.add("unexpected argument parallelism: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarClassName != targetCluster.sidecar.className) {
                        statusReport.add("unexpected argument class name: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarSavepoint != targetCluster.sidecar.savepoint) {
                        statusReport.add("unexpected argument savepoint: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarJarPath != targetCluster.sidecar.jarPath) {
                        statusReport.add("unexpected argument jar path: ${container.args.joinToString(separator = ",")}")
                    }

                    if (container.args.size > 1 && container.args.get(1) == "submit" && sidecarJarPath == null) {
                        statusReport.add("missing required jar path: ${container.args.joinToString(separator = ",")}")
                    }

                    val sidecarArguments = container.args.filter { it.startsWith("--argument") }.map { it.substringAfter("=") }.toList()

                    val arguments = if (sidecarArguments.isNotEmpty()) sidecarArguments.joinToString(" ") else null

                    if (arguments != targetCluster.sidecar.arguments) {
                        statusReport.add("sidecar arguments don't match: ${container.args.joinToString(separator = ",")}")
                    }
                }
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun extractArgument(containerArguments: List<String>, name: String) =
        containerArguments.filter { it.startsWith(name) }.map { it.substringAfter("=") }.firstOrNull()

    private fun evaluateJobManagerServiceStatus(
        actualClusterResources: ClusterResources,
        targetCluster: Cluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerService = actualClusterResources.jobmanagerService ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jobmanagerService.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["cluster"]?.equals(targetCluster.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["environment"]?.equals(targetCluster.descriptor.environment) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (jobmanagerService.spec.type != targetCluster.jobmanager.serviceMode) {
            statusReport.add("service mode doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateJobManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        targetCluster: Cluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerStatefulSet = actualClusterResources.jobmanagerStatefulSet ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jobmanagerStatefulSet.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["cluster"]?.equals(targetCluster.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["environment"]?.equals(targetCluster.descriptor.environment) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (jobmanagerStatefulSet.spec.template.spec.serviceAccountName != targetCluster.jobmanager.serviceAccount) {
            statusReport.add("service account does not match")
        }

        if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets.size != 1) {
            statusReport.add("unexpected number of pull secrets")
        } else {
            if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets[0].name != targetCluster.jobmanager.pullSecrets) {
                statusReport.add("pull secrets don't match")
            }
        }

        if (jobmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            statusReport.add("unexpected number of volume claim templates")
        } else {
            if (jobmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.storageClassName != targetCluster.jobmanager.storage.storageClass) {
                statusReport.add("volume claim storage class doesn't match")
            }

            if (jobmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.resources.requests.get("storage")?.number?.toInt()?.equals(targetCluster.jobmanager.storage.size) != true) {
                statusReport.add("volume claim size doesn't match")
            }
        }

        if (jobmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        } else {
            val container = jobmanagerStatefulSet.spec.template.spec.containers.get(0)

            if (container.image != targetCluster.jobmanager.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != targetCluster.jobmanager.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(targetCluster.jobmanager.resources.cpus) != true) {
                statusReport.add("container cpu limit doesn't match")
            }

            if (container.resources.requests.get("memory")?.number?.toInt()?.equals(targetCluster.jobmanager.resources.memory * 1024 * 1024) != true) {
                statusReport.add("container memory limit doesn't match")
            }

            val jobmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (jobmanagerRpcAddressEnvVar?.value == null || (actualClusterResources.jobmanagerService != null && jobmanagerRpcAddressEnvVar.value.toString() != "${actualClusterResources.jobmanagerService.metadata.name}")) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val jobmanagerEnvironmentEnvVar = container.env.filter { it.name == "FLINK_ENVIRONMENT" }.firstOrNull()

            if (jobmanagerEnvironmentEnvVar?.value == null || jobmanagerEnvironmentEnvVar.value.toString() != targetCluster.descriptor.environment) {
                statusReport.add("missing or invalid environment variable FLINK_ENVIRONMENT")
            }

            val jobmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_JM_HEAP" }.firstOrNull()

            if (jobmanagerMemoryEnvVar?.value == null || jobmanagerMemoryEnvVar.value.toInt() < targetCluster.jobmanager.resources.memory) {
                statusReport.add("missing or invalid environment variable FLINK_JM_HEAP")
            }

            val jobmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull()

            if (jobmanagerPodNamespaceEnvVar?.valueFrom == null || jobmanagerPodNamespaceEnvVar.valueFrom.fieldRef.fieldPath != "metadata.namespace") {
                statusReport.add("missing or invalid environment variable POD_NAMESPACE")
            }

            val jobmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull()

            if (jobmanagerPodNameEnvVar?.valueFrom == null || jobmanagerPodNameEnvVar.valueFrom.fieldRef.fieldPath != "metadata.name") {
                statusReport.add("missing or invalid environment variable POD_NAME")
            }

            val jobmanagerEnvironmentVariables = container.env
                .filter { it.name != "JOB_MANAGER_RPC_ADDRESS" }
                .filter { it.name != "FLINK_JM_HEAP" }
                .filter { it.name != "FLINK_ENVIRONMENT" }
                .filter { it.name != "POD_NAMESPACE" }
                .filter { it.name != "POD_NAME" }
                .map { EnvironmentVariable(it.name, it.value) }
                .toList()

            if (jobmanagerEnvironmentVariables != targetCluster.jobmanager.environmentVariables) {
                statusReport.add("container environment variables don't match")
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateTaskManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        targetCluster: Cluster
    ): Pair<ResourceStatus, List<String>> {
        val taskmanagerStatefulSet = actualClusterResources.taskmanagerStatefulSet ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (taskmanagerStatefulSet.metadata.labels["role"]?.equals("taskmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["cluster"]?.equals(targetCluster.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["environment"]?.equals(targetCluster.descriptor.environment) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (taskmanagerStatefulSet.spec.template.spec.serviceAccountName != targetCluster.taskmanager.serviceAccount) {
            statusReport.add("service account does not match")
        }

        if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets.size != 1) {
            statusReport.add("unexpected number of pull secrets")
        } else {
            if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets[0].name != targetCluster.taskmanager.pullSecrets) {
                statusReport.add("pull secrets don't match")
            }
        }

        if (taskmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            statusReport.add("unexpected number of volume claim templates")
        } else {
            if (taskmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.storageClassName != targetCluster.taskmanager.storage.storageClass) {
                statusReport.add("volume claim storage class doesn't match")
            }

            if (taskmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.resources.requests.get("storage")?.number?.toInt()?.equals(targetCluster.taskmanager.storage.size) != true) {
                statusReport.add("volume claim size doesn't match")
            }
        }

        if (taskmanagerStatefulSet.spec.replicas != targetCluster.taskmanager.replicas) {
            statusReport.add("number of replicas doesn't match")
        }

        if (taskmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        } else {
            val container = taskmanagerStatefulSet.spec.template.spec.containers.get(0)

            if (container.image != targetCluster.taskmanager.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != targetCluster.taskmanager.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(targetCluster.taskmanager.resources.cpus) != true) {
                statusReport.add("container cpu limit doesn't match")
            }

            if (container.resources.requests.get("memory")?.number?.toInt()?.equals(targetCluster.taskmanager.resources.memory * 1024 * 1024) != true) {
                statusReport.add("container memory limit doesn't match")
            }

            val taskmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (taskmanagerRpcAddressEnvVar?.value == null || (actualClusterResources.jobmanagerService != null && taskmanagerRpcAddressEnvVar.value.toString() != "${actualClusterResources.jobmanagerService.metadata.name}")) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val taskmanagerEnvironmentEnvVar = container.env.filter { it.name == "FLINK_ENVIRONMENT" }.firstOrNull()

            if (taskmanagerEnvironmentEnvVar?.value == null || taskmanagerEnvironmentEnvVar.value.toString() != targetCluster.descriptor.environment) {
                statusReport.add("missing or invalid environment variable FLINK_ENVIRONMENT")
            }

            val taskmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull()

            if (taskmanagerMemoryEnvVar?.value == null || taskmanagerMemoryEnvVar.value.toInt() < targetCluster.taskmanager.resources.memory) {
                statusReport.add("missing or invalid environment variable FLINK_TM_HEAP")
            }

            val taskmanagerTaskSlotsEnvVar = container.env.filter { it.name == "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }.firstOrNull()

            if (taskmanagerTaskSlotsEnvVar?.value == null || taskmanagerTaskSlotsEnvVar.value.toInt() != targetCluster.taskmanager.taskSlots) {
                statusReport.add("missing or invalid environment variable TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
            }

            val taskmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull()

            if (taskmanagerPodNamespaceEnvVar?.valueFrom == null || taskmanagerPodNamespaceEnvVar.valueFrom.fieldRef.fieldPath != "metadata.namespace") {
                statusReport.add("missing or invalid environment variable POD_NAMESPACE")
            }

            val taskmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull()

            if (taskmanagerPodNameEnvVar?.valueFrom == null || taskmanagerPodNameEnvVar.valueFrom.fieldRef.fieldPath != "metadata.name") {
                statusReport.add("missing or invalid environment variable POD_NAME")
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

            if (!taskmanagerEnvironmentVariables.equals(targetCluster.taskmanager.environmentVariables)) {
                statusReport.add("container environment variables don't match")
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateJobManagerPersistentVolumeClaimStatus(
        actualClusterResources: ClusterResources,
        targetCluster: Cluster
    ): Pair<ResourceStatus, List<String>> {
        val jobmanagerPersistentVolumeClaim = actualClusterResources.jobmanagerPersistentVolumeClaim ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (jobmanagerPersistentVolumeClaim.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["cluster"]?.equals(targetCluster.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["environment"]?.equals(targetCluster.descriptor.environment) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.spec.storageClassName != targetCluster.jobmanager.storage.storageClass) {
            statusReport.add("persistent volume storage class doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }

    private fun evaluateTaskManagerPersistentVolumeClaimStatus(
        actualClusterResources: ClusterResources,
        targetCluster: Cluster
    ): Pair<ResourceStatus, List<String>> {
        val taskmanagerPersistentVolumeClaim = actualClusterResources.taskmanagerPersistentVolumeClaim ?: return ResourceStatus.MISSING to listOf()

        val statusReport = mutableListOf<String>()

        if (taskmanagerPersistentVolumeClaim.metadata.labels["role"]?.equals("taskmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["cluster"]?.equals(targetCluster.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["environment"]?.equals(targetCluster.descriptor.environment) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.spec.storageClassName != targetCluster.taskmanager.storage.storageClass) {
            statusReport.add("persistent volume storage class doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT to statusReport
        }

        return ResourceStatus.VALID to listOf()
    }
}