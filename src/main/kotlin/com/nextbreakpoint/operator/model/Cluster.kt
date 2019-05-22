package com.nextbreakpoint.operator.model

import com.nextbreakpoint.model.ClusterDescriptor

data class Cluster(
    val descriptor: ClusterDescriptor,
    val jobmanager: JobManager,
    val taskmanager: TaskManager,
    val sidecar: Sidecar
)