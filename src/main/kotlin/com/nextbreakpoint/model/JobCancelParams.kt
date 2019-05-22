package com.nextbreakpoint.model

data class JobCancelParams(
    val descriptor: Descriptor,
    val savepointPath: String = "file:///var/tmp/savepoints",
    val savepoint: Boolean = false,
    val jobId: String
)