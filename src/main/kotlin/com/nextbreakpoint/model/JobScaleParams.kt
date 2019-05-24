package com.nextbreakpoint.model

data class JobScaleParams(
    val jobDescriptor: JobDescriptor,
    val parallelism: Int
)