package com.nextbreakpoint.operator

import com.nextbreakpoint.model.Descriptor
import com.nextbreakpoint.operator.model.JobManager
import com.nextbreakpoint.operator.model.Sidecar
import com.nextbreakpoint.operator.model.TaskManager
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

interface ClusterResourcesFactory {
    fun createJobManagerService(
        clusterOwner: String,
        descriptor: Descriptor,
        serviceMode: String
    ): V1Service?

    fun createSidecarDeployment(
        clusterOwner: String,
        descriptor: Descriptor,
        sidecar: Sidecar
    ): V1Deployment?

    fun createJobManagerStatefulSet(
        clusterOwner: String,
        descriptor: Descriptor,
        jobmanager: JobManager
    ): V1StatefulSet

    fun createTaskManagerStatefulSet(
        clusterOwner: String,
        descriptor: Descriptor,
        taskmanager: TaskManager
    ): V1StatefulSet?
}