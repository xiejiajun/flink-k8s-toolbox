package com.nextbreakpoint.operator

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.operator.model.*

object TestFactory {
    fun baseClusterConfig(): ClusterConfig {
        return ClusterConfig(
            descriptor = ClusterDescriptor(
                namespace = "testNamespace",
                name = "testCluster",
                environment = "testEnvironment"
            ),
            jobmanager = JobManagerConfig(
                image = "flink:1.7.2",
                pullPolicy = "Always",
                pullSecrets = "somesecrets",
                serviceMode = "ClusterIP",
                serviceAccount = "testServiceAccount",
                environmentVariables = listOf(EnvironmentVariable("key", "value")),
                storage = StorageConfig(
                    size = 100,
                    storageClass = "testStorageClass"
                ),
                resources = ResourcesConfig(
                    cpus = 1.0f,
                    memory = 500
                )
            ),
            taskmanager = TaskManagerConfig(
                image = "flink:1.7.2",
                pullPolicy = "Always",
                pullSecrets = "somesecrets",
                serviceAccount = "testServiceAccount",
                taskSlots = 1,
                replicas = 2,
                environmentVariables = listOf(EnvironmentVariable("key", "value")),
                storage = StorageConfig(
                    size = 100,
                    storageClass = "testStorageClass"
                ),
                resources = ResourcesConfig(
                    cpus = 1.0f,
                    memory = 1000
                )
            ),
            sidecar = SidecarConfig(
                image = "sidecar:1.0",
                pullPolicy = "Always",
                serviceAccount = "testServiceAccount",
                pullSecrets = "somesecrets",
                jarPath = "test.jar",
                className = "test.TestJob",
                savepoint = "somesavepoint",
                arguments = "--key=value",
                parallelism = 1
            )
        )
    }
}