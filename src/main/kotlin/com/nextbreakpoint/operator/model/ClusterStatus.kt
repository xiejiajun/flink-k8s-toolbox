package com.nextbreakpoint.operator.model

data class ClusterStatus(
    val jobmanagerService: ResourceStatus,
    val sidecarDeployment: ResourceStatus,
    val jobmanagerStatefulSet: ResourceStatus,
    val taskmanagerStatefulSet: ResourceStatus,
    val jobmanagerPersistentVolumeClaim: ResourceStatus,
    val taskmanagerPersistentVolumeClaim: ResourceStatus
)

