package com.nextbreakpoint.operator.model

data class TaskManager(
    val image: String,
    val pullSecrets: String?,
    val pullPolicy: String,
    val serviceAccount: String,
    val taskSlots: Int,
    val replicas: Int,
    val storage: Storage,
    val resources: Resources,
    val environmentVariables: List<EnvironmentVariable>
)