package com.nextbreakpoint.model

data class JobScaleParams(
    val descriptor: Descriptor,
    val parallelism: Int,
    val jobId: String
)