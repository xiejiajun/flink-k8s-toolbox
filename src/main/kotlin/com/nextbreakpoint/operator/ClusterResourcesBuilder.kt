package com.nextbreakpoint.operator

import com.nextbreakpoint.operator.model.Cluster
import com.nextbreakpoint.operator.model.ClusterResources

class ClusterResourcesBuilder(
    private val factory: ClusterResourcesFactory = DefaultClusterResourcesFactory,
    private val clusterOwner: String,
    private val cluster: Cluster
) {
    fun build(): ClusterResources {
        val jobmanagerService = factory.createJobManagerService(
            clusterOwner,
            cluster.descriptor,
            cluster.jobmanager.serviceMode
        )

        val jobmanagerStatefulSet = factory.createJobManagerStatefulSet(
            clusterOwner,
            cluster.descriptor,
            cluster.jobmanager
        )

        val sidecarDeployment = factory.createSidecarDeployment(
            clusterOwner,
            cluster.descriptor,
            cluster.sidecar
        )

        val taskmanagerStatefulSet = factory.createTaskManagerStatefulSet(
            clusterOwner,
            cluster.descriptor,
            cluster.taskmanager
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