package com.nextbreakpoint.handler.model

import com.nextbreakpoint.common.model.Descriptor

data class JobDescriptor(
    val descriptor: Descriptor,
    val jobId: String
)