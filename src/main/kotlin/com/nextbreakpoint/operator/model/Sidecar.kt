package com.nextbreakpoint.operator.model

data class Sidecar(
    val image: String,
    val pullSecrets: String?,
    val pullPolicy: String,
    val arguments: String?,
    val className: String?,
    val jarPath: String?,
    val savepoint: String?,
    val serviceAccount: String,
    val parallelism: Int
)