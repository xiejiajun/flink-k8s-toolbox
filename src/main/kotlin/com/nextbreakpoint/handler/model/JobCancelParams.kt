package com.nextbreakpoint.handler.model

data class JobCancelParams(
    val jobDescriptor: JobDescriptor,
    val savepointPath: String,
    val savepoint: Boolean
)