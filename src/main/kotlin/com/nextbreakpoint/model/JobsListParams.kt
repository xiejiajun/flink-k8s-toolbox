package com.nextbreakpoint.model

data class JobsListParams(
    val descriptor: Descriptor,
    val savepoint: Boolean = false,
    val running: Boolean = false
)