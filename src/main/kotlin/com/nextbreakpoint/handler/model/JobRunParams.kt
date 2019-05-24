package com.nextbreakpoint.handler.model

import com.nextbreakpoint.common.model.Descriptor
import com.nextbreakpoint.operator.model.Sidecar

data class JobRunParams(
    val descriptor: Descriptor,
    val sidecar: Sidecar
)