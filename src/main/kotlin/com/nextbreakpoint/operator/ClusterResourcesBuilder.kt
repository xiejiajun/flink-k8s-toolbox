package com.nextbreakpoint.operator

import com.nextbreakpoint.operator.model.ClusterConfig
import com.nextbreakpoint.operator.model.ClusterResources
import io.kubernetes.client.models.*

class ClusterResourcesBuilder(
    private val clusterOwner: String,
    private val clusterConfig: ClusterConfig
) {
    fun build(): ClusterResources {
        val srvPort8081 = ClusterResourcesFactory.createServicePort(8081, "ui")
        val srvPort6123 = ClusterResourcesFactory.createServicePort(6123, "rpc")
        val srvPort6124 = ClusterResourcesFactory.createServicePort(6124, "blob")
        val srvPort6125 = ClusterResourcesFactory.createServicePort(6125, "query")

        val port8081 = ClusterResourcesFactory.createContainerPort(8081, "ui")
        val port6121 = ClusterResourcesFactory.createContainerPort(6121, "data")
        val port6122 = ClusterResourcesFactory.createContainerPort(6122, "ipc")
        val port6123 = ClusterResourcesFactory.createContainerPort(6123, "rpc")
        val port6124 = ClusterResourcesFactory.createContainerPort(6124, "blob")
        val port6125 = ClusterResourcesFactory.createContainerPort(6125, "query")

        val componentLabel = Pair("component", "flink")

        val environmentLabel = Pair("environment", clusterConfig.descriptor.environment)

        val clusterLabel = Pair("cluster", clusterConfig.descriptor.name)

        val ownerLabel = Pair("owner", clusterOwner)

        val jobmanagerLabel = Pair("role", "jobmanager")

        val taskmanagerLabel = Pair("role", "taskmanager")

        val jobmanagerResources = clusterConfig.jobmanager.resources

        val taskmanagerResources = clusterConfig.taskmanager.resources

        val jobmanagerResourceRequirements = ClusterResourcesFactory.createResourceRequirements(jobmanagerResources)

        val taskmanagerResourceRequirements = ClusterResourcesFactory.createResourceRequirements(taskmanagerResources)

        val jobmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

        val taskmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, taskmanagerLabel, environmentLabel)

        val sidecarLabels = mapOf(ownerLabel, clusterLabel, componentLabel, environmentLabel)

        val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

        val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

        val sidecarSelector = V1LabelSelector().matchLabels(sidecarLabels)

        val environmentEnvVar = ClusterResourcesFactory.createEnvVar(
            "FLINK_ENVIRONMENT",
            clusterConfig.descriptor.environment
        )

        val jobManagerHeapEnvVar = ClusterResourcesFactory.createEnvVar(
            "FLINK_JM_HEAP",
            jobmanagerResources.memory.toString()
        )

        val taskManagerHeapEnvVar = ClusterResourcesFactory.createEnvVar(
            "FLINK_TM_HEAP",
            taskmanagerResources.memory.toString()
        )

        val numberOfTaskSlotsEnvVar = ClusterResourcesFactory.createEnvVar(
            "TASK_MANAGER_NUMBER_OF_TASK_SLOTS",
            clusterConfig.taskmanager.taskSlots.toString()
        )

        val rpcAddressEnvVar = ClusterResourcesFactory.createEnvVar(
            "JOB_MANAGER_RPC_ADDRESS",
            "flink-jobmanager-${clusterConfig.descriptor.name}"
        )

        val podNameEnvVar = ClusterResourcesFactory.createEnvVarFromField(
            "POD_NAME",
            "metadata.name"
        )

        val podNamespaceEnvVar = ClusterResourcesFactory.createEnvVarFromField(
            "POD_NAMESPACE",
            "metadata.namespace"
        )

        val jobmanagerVolumeMount = ClusterResourcesFactory.createVolumeMount("jobmanager")

        val taskmanagerVolumeMount = ClusterResourcesFactory.createVolumeMount("taskmanager")

        val updateStrategy = V1StatefulSetUpdateStrategy().type("RollingUpdate")

        val jobmanagerAffinity = ClusterResourcesFactory.createAffinity(
            jobmanagerSelector,
            taskmanagerSelector
        )

        val jobmanagerService = ClusterResourcesFactory.createJobManagerService(
            srvPort8081,
            srvPort6123,
            srvPort6124,
            srvPort6125,
            jobmanagerLabels,
            clusterConfig.descriptor.name,
            clusterConfig.jobmanager.serviceMode
        )

        val jobmanagerStatefulSet = ClusterResourcesFactory.createJobManagerStatefulSet(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            rpcAddressEnvVar,
            jobManagerHeapEnvVar,
            port8081,
            port6123,
            port6124,
            port6125,
            jobmanagerVolumeMount,
            jobmanagerResourceRequirements,
            jobmanagerAffinity,
            jobmanagerSelector,
            jobmanagerLabels,
            updateStrategy,
            clusterConfig.jobmanager.environmentVariables,
            clusterConfig.descriptor.name,
            clusterConfig.jobmanager.image,
            clusterConfig.jobmanager.pullPolicy,
            clusterConfig.jobmanager.pullSecrets,
            clusterConfig.jobmanager.serviceAccount,
            clusterConfig.jobmanager.storage
        )

        val sidecarDeployment = ClusterResourcesFactory.createSidecarDeployment(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            jobmanagerAffinity,
            sidecarLabels,
            sidecarSelector,
            clusterConfig.descriptor.namespace,
            clusterConfig.descriptor.environment,
            clusterConfig.descriptor.name,
            clusterConfig.sidecar.image,
            clusterConfig.sidecar.pullPolicy,
            clusterConfig.sidecar.pullSecrets,
            clusterConfig.sidecar.serviceAccount,
            clusterConfig.sidecar.className,
            clusterConfig.sidecar.jarPath,
            clusterConfig.sidecar.arguments,
            clusterConfig.sidecar.savepoint,
            clusterConfig.sidecar.parallelism
        )

        val taskmanagerStatefulSet = ClusterResourcesFactory.createTaskManagerStatefulSet(
            podNameEnvVar,
            podNamespaceEnvVar,
            environmentEnvVar,
            rpcAddressEnvVar,
            taskManagerHeapEnvVar,
            numberOfTaskSlotsEnvVar,
            port6121,
            port6122,
            taskmanagerVolumeMount,
            taskmanagerResourceRequirements,
            jobmanagerSelector,
            taskmanagerSelector,
            taskmanagerLabels,
            updateStrategy,
            clusterConfig.taskmanager.environmentVariables,
            clusterConfig.descriptor.name,
            clusterConfig.taskmanager.replicas,
            clusterConfig.taskmanager.image,
            clusterConfig.taskmanager.pullPolicy,
            clusterConfig.taskmanager.pullSecrets,
            clusterConfig.taskmanager.serviceAccount,
            clusterConfig.taskmanager.storage
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
}