package com.nextbreakpoint.operator.model

import com.nextbreakpoint.model.Descriptor

data class Cluster(
    val descriptor: Descriptor,
    val jobmanager: JobManager,
    val taskmanager: TaskManager,
    val sidecar: Sidecar
)