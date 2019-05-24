package com.nextbreakpoint.handler.model

import com.nextbreakpoint.common.model.Descriptor

data class JobSubmitParams(
    val descriptor: Descriptor,
    val jarPath: String,
    val className: String?,
    val arguments: String?,
    val savepoint: String?,
    val parallelism: Int
)