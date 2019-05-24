package com.nextbreakpoint.handler.model

import com.nextbreakpoint.common.model.Descriptor

data class JobsListParams(
    val descriptor: Descriptor,
    val running: Boolean
)