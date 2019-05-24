package com.nextbreakpoint.handler.model

data class JobScaleParams(
    val jobDescriptor: JobDescriptor,
    val parallelism: Int
)