package com.nextbreakpoint.operator.model

import com.nextbreakpoint.model.ClusterDescriptor

data class ClusterConfig(
    val descriptor: ClusterDescriptor,
    val jobmanager: JobManagerConfig,
    val taskmanager: TaskManagerConfig,
    val sidecar: SidecarConfig
)