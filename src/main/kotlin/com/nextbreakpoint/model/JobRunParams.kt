package com.nextbreakpoint.model

import com.nextbreakpoint.operator.model.SidecarConfig

data class JobRunParams(
    val descriptor: ClusterDescriptor,
    val sidecar: SidecarConfig
)