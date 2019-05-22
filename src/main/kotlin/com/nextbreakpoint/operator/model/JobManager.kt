package com.nextbreakpoint.operator.model

data class JobManager(
    val image: String,
    val pullSecrets: String?,
    val pullPolicy: String,
    val storage: Storage,
    val resources: Resources,
    val serviceMode: String,
    val serviceAccount: String,
    val environmentVariables: List<EnvironmentVariable>
)