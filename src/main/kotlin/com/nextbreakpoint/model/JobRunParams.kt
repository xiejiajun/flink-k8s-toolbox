package com.nextbreakpoint.model

import com.nextbreakpoint.operator.model.Sidecar

data class JobRunParams(
    val descriptor: Descriptor,
    val sidecar: Sidecar
)