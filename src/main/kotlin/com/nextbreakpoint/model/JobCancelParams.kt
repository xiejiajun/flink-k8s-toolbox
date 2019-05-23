package com.nextbreakpoint.model

data class JobCancelParams(
    val descriptor: Descriptor,
    val savepointPath: String,
    val savepoint: Boolean,
    val jobId: String
)