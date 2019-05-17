package com.nextbreakpoint.operator.model

data class ClusterStatus(
    val jobmanagerService: Pair<ResourceStatus, List<String>>,
    val sidecarDeployment: Pair<ResourceStatus, List<String>>,
    val jobmanagerStatefulSet: Pair<ResourceStatus, List<String>>,
    val taskmanagerStatefulSet: Pair<ResourceStatus, List<String>>,
    val jobmanagerPersistentVolumeClaim: Pair<ResourceStatus, List<String>>,
    val taskmanagerPersistentVolumeClaim: Pair<ResourceStatus, List<String>>
)

