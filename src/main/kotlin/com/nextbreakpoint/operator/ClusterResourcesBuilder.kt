package com.nextbreakpoint.operator

import com.nextbreakpoint.operator.model.ClusterConfig
import com.nextbreakpoint.operator.model.ClusterResources

class ClusterResourcesBuilder(
    private val factory: ClusterResourcesFactory = DefaultClusterResourcesFactory,
    private val clusterOwner: String,
    private val clusterConfig: ClusterConfig
) {
    fun build(): ClusterResources {
        val jobmanagerService = factory.createJobManagerService(
            clusterOwner,
            clusterConfig.descriptor,
            clusterConfig.jobmanager.serviceMode
        )

        val jobmanagerStatefulSet = factory.createJobManagerStatefulSet(
            clusterOwner,
            clusterConfig.descriptor,
            clusterConfig.jobmanager
        )

        val sidecarDeployment = factory.createSidecarDeployment(
            clusterOwner,
            clusterConfig.descriptor,
            clusterConfig.sidecar
        )

        val taskmanagerStatefulSet = factory.createTaskManagerStatefulSet(
            clusterOwner,
            clusterConfig.descriptor,
            clusterConfig.taskmanager
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