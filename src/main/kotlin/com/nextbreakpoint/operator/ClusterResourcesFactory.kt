package com.nextbreakpoint.operator

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.operator.model.JobManagerConfig
import com.nextbreakpoint.operator.model.SidecarConfig
import com.nextbreakpoint.operator.model.TaskManagerConfig
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

interface ClusterResourcesFactory {
    fun createJobManagerService(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        serviceMode: String
    ): V1Service?

    fun createSidecarDeployment(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        sidecarConfig: SidecarConfig
    ): V1Deployment?

    fun createJobManagerStatefulSet(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        jobmanagerConfig: JobManagerConfig
    ): V1StatefulSet

    fun createTaskManagerStatefulSet(
        clusterOwner: String,
        descriptor: ClusterDescriptor,
        taskmanagerConfig: TaskManagerConfig
    ): V1StatefulSet?
}