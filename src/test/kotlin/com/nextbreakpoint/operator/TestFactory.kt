package com.nextbreakpoint.operator

import com.nextbreakpoint.model.Descriptor
import com.nextbreakpoint.operator.model.*

object TestFactory {
    fun aCluster(): Cluster {
        return Cluster(
            descriptor = Descriptor(
                namespace = "testNamespace",
                name = "testCluster"
            ),
            jobmanager = JobManager(
                image = "flink:1.7.2",
                pullPolicy = "Always",
                pullSecrets = "somesecrets",
                serviceMode = "ClusterIP",
                serviceAccount = "testServiceAccount",
                environment = listOf(EnvironmentVariable("key", "value")),
                storage = Storage(
                    size = 100,
                    storageClass = "testStorageClass"
                ),
                resources = Resources(
                    cpus = 1.0f,
                    memory = 500
                )
            ),
            taskmanager = TaskManager(
                image = "flink:1.7.2",
                pullPolicy = "Always",
                pullSecrets = "somesecrets",
                serviceAccount = "testServiceAccount",
                taskSlots = 1,
                replicas = 2,
                environment = listOf(EnvironmentVariable("key", "value")),
                storage = Storage(
                    size = 100,
                    storageClass = "testStorageClass"
                ),
                resources = Resources(
                    cpus = 1.0f,
                    memory = 1000
                )
            ),
            sidecar = Sidecar(
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