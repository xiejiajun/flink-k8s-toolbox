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

        val statusReport = mutableListOf<String>()

        if (sidecarDeployment.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (sidecarDeployment.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (sidecarDeployment.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (sidecarDeployment.spec.template.spec.serviceAccount != targetClusterConfig.sidecar.serviceAccount) {
            statusReport.add("service account does not match")
        }

        if (sidecarDeployment.spec.template.spec.imagePullSecrets.size != 1) {
            statusReport.add("unexpected number of pull secrets")
        } else {
            if (sidecarDeployment.spec.template.spec.imagePullSecrets[0].name != targetClusterConfig.sidecar.pullSecrets) {
                statusReport.add("pull secrets don't match")
            }
        }

        if (sidecarDeployment.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        } else {
            val container = sidecarDeployment.spec.template.spec.containers.get(0)

            if (container.image != targetClusterConfig.sidecar.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != targetClusterConfig.sidecar.pullPolicy) {
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

                    if (sidecarNamespace == null || sidecarNamespace != targetClusterConfig.descriptor.namespace) {
                        statusReport.add("unexpected argument namespace: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarEnvironment == null || sidecarEnvironment != targetClusterConfig.descriptor.environment) {
                        statusReport.add("unexpected argument environment: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarClusterName == null || sidecarClusterName != targetClusterConfig.descriptor.name) {
                        statusReport.add("unexpected argument cluster name: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarParallelism == null || sidecarParallelism.toInt() != targetClusterConfig.sidecar.parallelism) {
                        statusReport.add("unexpected argument parallelism: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarClassName != targetClusterConfig.sidecar.className) {
                        statusReport.add("unexpected argument class name: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarSavepoint != targetClusterConfig.sidecar.savepoint) {
                        statusReport.add("unexpected argument savepoint: ${container.args.joinToString(separator = ",")}")
                    }

                    if (sidecarJarPath != targetClusterConfig.sidecar.jarPath) {
                        statusReport.add("unexpected argument jar path: ${container.args.joinToString(separator = ",")}")
                    }

                    if (container.args.size > 1 && container.args.get(1) == "submit" && sidecarJarPath == null) {
                        statusReport.add("missing required jar path: ${container.args.joinToString(separator = ",")}")
                    }

                    val sidecarArguments = container.args.filter { it.startsWith("--argument") }.map { it.substringAfter("=") }.toList()

                    val arguments = if (sidecarArguments.isNotEmpty()) sidecarArguments.joinToString(" ") else null

                    if (arguments != targetClusterConfig.sidecar.arguments) {
                        statusReport.add("sidecar arguments don't match: ${container.args.joinToString(separator = ",")}")
                    }
                }
            }
        }

        if (statusReport.size > 0) {
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

        val statusReport = mutableListOf<String>()

        if (jobmanagerService.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (jobmanagerService.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (jobmanagerService.spec.type != targetClusterConfig.jobmanager.serviceMode) {
            statusReport.add("service mode doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateJobManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val jobmanagerStatefulSet = actualClusterResources.jobmanagerStatefulSet ?: return ResourceStatus.MISSING

        val statusReport = mutableListOf<String>()

        if (jobmanagerStatefulSet.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (jobmanagerStatefulSet.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (jobmanagerStatefulSet.spec.template.spec.serviceAccount != targetClusterConfig.jobmanager.serviceAccount) {
            statusReport.add("service account does not match")
        }

        if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets.size != 1) {
            statusReport.add("unexpected number of pull secrets")
        } else {
            if (jobmanagerStatefulSet.spec.template.spec.imagePullSecrets[0].name != targetClusterConfig.jobmanager.pullSecrets) {
                statusReport.add("pull secrets don't match")
            }
        }

        if (jobmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            statusReport.add("unexpected number of volume claim templates")
        } else {
            if (jobmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.storageClassName != targetClusterConfig.jobmanager.storage.storageClass) {
                statusReport.add("volume claim storage class doesn't match")
            }

            if (jobmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.resources.requests.get("storage")?.number?.toInt()?.equals(targetClusterConfig.jobmanager.storage.size) != true) {
                statusReport.add("volume claim size doesn't match")
            }
        }

        if (jobmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        } else {
            val container = jobmanagerStatefulSet.spec.template.spec.containers.get(0)

            if (container.image != targetClusterConfig.jobmanager.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != targetClusterConfig.jobmanager.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(targetClusterConfig.jobmanager.resources.cpus) != true) {
                statusReport.add("container cpu limit doesn't match")
            }

            val jobmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (jobmanagerRpcAddressEnvVar == null || (actualClusterResources.jobmanagerService != null && jobmanagerRpcAddressEnvVar.value.toString() != "${actualClusterResources.jobmanagerService.metadata.name}:8081")) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val jobmanagerEnvironmentEnvVar = container.env.filter { it.name == "FLINK_ENVIRONMENT" }.firstOrNull()

            if (jobmanagerEnvironmentEnvVar == null || jobmanagerEnvironmentEnvVar.value.toString() != targetClusterConfig.descriptor.environment) {
                statusReport.add("missing or invalid environment variable FLINK_ENVIRONMENT")
            }

            val jobmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull()

            if (jobmanagerMemoryEnvVar == null || jobmanagerMemoryEnvVar.value.toInt() < targetClusterConfig.jobmanager.resources.memory) {
                statusReport.add("missing or invalid environment variable FLINK_TM_HEAP")
            }

            val jobmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull()

            if (jobmanagerPodNamespaceEnvVar == null || jobmanagerPodNamespaceEnvVar.value.toString() != actualClusterResources.jobmanagerStatefulSet.metadata.namespace) {
                statusReport.add("missing or invalid environment variable POD_NAMESPACE")
            }

            val jobmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull()

            if (jobmanagerPodNameEnvVar == null || jobmanagerPodNameEnvVar.value.toString() != actualClusterResources.jobmanagerStatefulSet.metadata.name) {
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

            if (jobmanagerEnvironmentVariables != targetClusterConfig.jobmanager.environmentVariables) {
                statusReport.add("container environment variables don't match")
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateTaskManagerStatefulSetStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val taskmanagerStatefulSet = actualClusterResources.taskmanagerStatefulSet ?: return ResourceStatus.MISSING

        val statusReport = mutableListOf<String>()

        if (taskmanagerStatefulSet.metadata.labels["role"]?.equals("taskmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (taskmanagerStatefulSet.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (taskmanagerStatefulSet.spec.template.spec.serviceAccount != targetClusterConfig.taskmanager.serviceAccount) {
            statusReport.add("service account does not match")
        }

        if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets.size != 1) {
            statusReport.add("unexpected number of pull secrets")
        } else {
            if (taskmanagerStatefulSet.spec.template.spec.imagePullSecrets[0].name != targetClusterConfig.taskmanager.pullSecrets) {
                statusReport.add("pull secrets don't match")
            }
        }

        if (taskmanagerStatefulSet.spec.volumeClaimTemplates.size != 1) {
            statusReport.add("unexpected number of volume claim templates")
        } else {
            if (taskmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.storageClassName != targetClusterConfig.taskmanager.storage.storageClass) {
                statusReport.add("volume claim storage class doesn't match")
            }

            if (taskmanagerStatefulSet.spec.volumeClaimTemplates[0].spec.resources.requests.get("storage")?.number?.toInt()?.equals(targetClusterConfig.taskmanager.storage.size) != true) {
                statusReport.add("volume claim size doesn't match")
            }
        }

        if (taskmanagerStatefulSet.spec.replicas != targetClusterConfig.taskmanager.replicas) {
            statusReport.add("number of replicas doesn't match")
        }

        if (taskmanagerStatefulSet.spec.template.spec.containers.size != 1) {
            statusReport.add("unexpected number of containers")
        } else {
            val container = taskmanagerStatefulSet.spec.template.spec.containers.get(0)

            if (container.image != targetClusterConfig.taskmanager.image) {
                statusReport.add("container image does not match")
            }

            if (container.imagePullPolicy != targetClusterConfig.taskmanager.pullPolicy) {
                statusReport.add("container image pull policy does not match")
            }

            if (container.resources.limits.get("cpu")?.number?.toFloat()?.equals(targetClusterConfig.taskmanager.resources.cpus) != true) {
                statusReport.add("container cpu limit doesn't match")
            }

            val taskmanagerRpcAddressEnvVar = container.env.filter { it.name == "JOB_MANAGER_RPC_ADDRESS" }.firstOrNull()

            if (taskmanagerRpcAddressEnvVar == null || (actualClusterResources.jobmanagerService != null && taskmanagerRpcAddressEnvVar.value.toString() != "${actualClusterResources.jobmanagerService.metadata.name}:8081")) {
                statusReport.add("missing or invalid environment variable JOB_MANAGER_RPC_ADDRESS")
            }

            val taskmanagerEnvironmentEnvVar = container.env.filter { it.name == "FLINK_ENVIRONMENT" }.firstOrNull()

            if (taskmanagerEnvironmentEnvVar == null || taskmanagerEnvironmentEnvVar.value.toString() != targetClusterConfig.descriptor.environment) {
                statusReport.add("missing or invalid environment variable FLINK_ENVIRONMENT")
            }

            val taskmanagerMemoryEnvVar = container.env.filter { it.name == "FLINK_TM_HEAP" }.firstOrNull()

            if (taskmanagerMemoryEnvVar == null || taskmanagerMemoryEnvVar.value.toInt() < targetClusterConfig.taskmanager.resources.memory) {
                statusReport.add("missing or invalid environment variable FLINK_TM_HEAP")
            }

            val taskmanagerTaskSlotsEnvVar = container.env.filter { it.name == "TASK_MANAGER_NUMBER_OF_TASK_SLOTS" }.firstOrNull()

            if (taskmanagerTaskSlotsEnvVar == null || taskmanagerTaskSlotsEnvVar.value.toInt() != targetClusterConfig.taskmanager.taskSlots) {
                statusReport.add("missing or invalid environment variable TASK_MANAGER_NUMBER_OF_TASK_SLOTS")
            }

            val taskmanagerPodNamespaceEnvVar = container.env.filter { it.name == "POD_NAMESPACE" }.firstOrNull()

            if (taskmanagerPodNamespaceEnvVar == null || taskmanagerPodNamespaceEnvVar.value.toString() != actualClusterResources.taskmanagerStatefulSet.metadata.namespace) {
                statusReport.add("missing or invalid environment variable POD_NAMESPACE")
            }

            val taskmanagerPodNameEnvVar = container.env.filter { it.name == "POD_NAME" }.firstOrNull()

            if (taskmanagerPodNameEnvVar == null || taskmanagerPodNameEnvVar.value.toString() != actualClusterResources.taskmanagerStatefulSet.metadata.name) {
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

            if (!taskmanagerEnvironmentVariables.equals(targetClusterConfig.taskmanager.environmentVariables)) {
                statusReport.add("container environment variables don't match")
            }
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateJobManagerPersistentVolumeClaimStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val jobmanagerPersistentVolumeClaim = actualClusterResources.jobmanagerPersistentVolumeClaim ?: return ResourceStatus.MISSING

        val statusReport = mutableListOf<String>()

        if (jobmanagerPersistentVolumeClaim.metadata.labels["role"]?.equals("jobmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (jobmanagerPersistentVolumeClaim.spec.storageClassName != targetClusterConfig.jobmanager.storage.storageClass) {
            statusReport.add("persistent volume storage class doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }

    private fun evaluateTaskManagerPersistentVolumeClaimStatus(
        actualClusterResources: ClusterResources,
        targetClusterConfig: ClusterConfig
    ): ResourceStatus {
        val taskmanagerPersistentVolumeClaim = actualClusterResources.taskmanagerPersistentVolumeClaim ?: return ResourceStatus.MISSING

        val statusReport = mutableListOf<String>()

        if (taskmanagerPersistentVolumeClaim.metadata.labels["role"]?.equals("taskmanager") != true) {
            statusReport.add("role label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["component"]?.equals("flink") != true) {
            statusReport.add("component label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["cluster"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("cluster label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.metadata.labels["environment"]?.equals(targetClusterConfig.descriptor.name) != true) {
            statusReport.add("environment label missing or invalid")
        }

        if (taskmanagerPersistentVolumeClaim.spec.storageClassName != targetClusterConfig.taskmanager.storage.storageClass) {
            statusReport.add("persistent volume storage class doesn't match")
        }

        if (statusReport.size > 0) {
            return ResourceStatus.DIVERGENT
        }

        return ResourceStatus.VALID
    }
}