package com.nextbreakpoint.operator.model

import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class ClusterResources(
    val jobmanagerService: V1Service?,
    val sidecarDeployment: V1Deployment?,
    val jobmanagerStatefulSet: V1StatefulSet?,
    val taskmanagerStatefulSet: V1StatefulSet?,
    val jobmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?,
    val taskmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?
) {
    fun withJobManagerService(jobmanagerService: V1Service?) =
        ClusterResources(
            jobmanagerService = jobmanagerService,
            sidecarDeployment = this.sidecarDeployment,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withSidecarDeployment(sidecarDeployment: V1Deployment?) =
        ClusterResources(
            jobmanagerService = this.jobmanagerService,
            sidecarDeployment = sidecarDeployment,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withJobManagerStatefulSet(jobmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            jobmanagerService = this.jobmanagerService,
            sidecarDeployment = this.sidecarDeployment,
            jobmanagerStatefulSet = jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withTaskManagerStatefulSet(taskmanagerStatefulSet: V1StatefulSet?) =
        ClusterResources(
            jobmanagerService = this.jobmanagerService,
            sidecarDeployment = this.sidecarDeployment,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withJobManagerPersistenVolumeClaim(jobmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?) =
        ClusterResources(
            jobmanagerService = this.jobmanagerService,
            sidecarDeployment = this.sidecarDeployment,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = this.taskmanagerPersistentVolumeClaim
        )

    fun withTaskManagerPersistenVolumeClaim(taskmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?) =
        ClusterResources(
            jobmanagerService = this.jobmanagerService,
            sidecarDeployment = this.sidecarDeployment,
            jobmanagerStatefulSet = this.jobmanagerStatefulSet,
            taskmanagerStatefulSet = this.taskmanagerStatefulSet,
            jobmanagerPersistentVolumeClaim = this.jobmanagerPersistentVolumeClaim,
            taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaim
        )
}

