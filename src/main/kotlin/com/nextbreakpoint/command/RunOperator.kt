package com.nextbreakpoint.command

import com.nextbreakpoint.handler.ClusterCreateHandler
import com.nextbreakpoint.handler.ClusterDeleteHandler
import com.nextbreakpoint.model.*
import com.nextbreakpoint.operator.*
import com.nextbreakpoint.operator.model.ClusterConfig
import com.nextbreakpoint.operator.ClusterConfigBuilder
import com.nextbreakpoint.operator.model.ClusterResources
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.apis.CustomObjectsApi
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class RunOperator {
    companion object {
        val logger = Logger.getLogger(RunOperator::class.simpleName)
    }

    private val sharedLock = Semaphore(1)
    private val queue = LinkedBlockingQueue<String>()
    private val clusters = mutableMapOf<ClusterDescriptor, V1FlinkCluster>()
    private val status = mutableMapOf<ClusterDescriptor, Long>()
    private val services = mutableMapOf<ClusterDescriptor, V1Service>()
    private val deployments = mutableMapOf<ClusterDescriptor, V1Deployment>()
    private val jobmanagerStatefulSets = mutableMapOf<ClusterDescriptor, V1StatefulSet>()
    private val taskmanagerStatefulSets = mutableMapOf<ClusterDescriptor, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = mutableMapOf<ClusterDescriptor, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = mutableMapOf<ClusterDescriptor, V1PersistentVolumeClaim>()

    fun run(config: OperatorConfig) {
        RunController.logger.info("Launching operator...")

        val objectApi = CustomObjectsApi()

        val coreApi = CoreV1Api()

        val appsApi = AppsV1Api()

        thread {
            while (true) {
                try {
                    val watch = ClusterResourcesWatchFactory.createWatchFlickClusterResources(config.namespace, objectApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.spec.clusterName
                        val environment = resource.`object`.spec.environment
                        if (clusterName != null && environment != null) {
                            sharedLock.acquire()
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> clusters.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                "DELETED" -> clusters.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = ClusterResourcesWatchFactory.createWatchServiceResources(config.namespace, coreApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        if (clusterName != null && environment != null) {
                            sharedLock.acquire()
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> services.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                "DELETED" -> services.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = ClusterResourcesWatchFactory.createWatchDeploymentResources(config.namespace, appsApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        if (clusterName != null && environment != null) {
                            sharedLock.acquire()
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> deployments.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                "DELETED" -> deployments.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = ClusterResourcesWatchFactory.createWatchStatefulSetResources(config.namespace, appsApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        val role = resource.`object`.metadata.labels.get("role")
                        if (clusterName != null && environment != null && role != null) {
                            sharedLock.acquire()
                            if (role.equals("jobmanager")) {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> jobmanagerStatefulSets.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> jobmanagerStatefulSets.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            } else {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> taskmanagerStatefulSets.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> taskmanagerStatefulSets.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        thread {
            while (true) {
                try {
                    val watch = ClusterResourcesWatchFactory.createWatchPermanentVolumeClaimResources(config.namespace, coreApi)

                    watch.forEach { resource ->
                        val clusterName = resource.`object`.metadata.labels.get("cluster")
                        val environment = resource.`object`.metadata.labels.get("environment")
                        val role = resource.`object`.metadata.labels.get("role")
                        if (clusterName != null && environment != null && role != null) {
                            sharedLock.acquire()
                            if (role.equals("jobmanager")) {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> jobmanagerPersistentVolumeClaims.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> jobmanagerPersistentVolumeClaims.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            } else {
                                when (resource.type) {
                                    "ADDED", "MODIFIED" -> taskmanagerPersistentVolumeClaims.put(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment), resource.`object`)
                                    "DELETED" -> taskmanagerPersistentVolumeClaims.remove(ClusterDescriptor(namespace = config.namespace, name = clusterName, environment = environment))
                                }
                            }
                            queue.add(resource.`object`.kind)
                            sharedLock.release()
                        }
                    }
                } catch (e : InterruptedException) {
                    break
                } catch (e : RuntimeException) {
                    if (!(e.cause is SocketTimeoutException)) {
                        e.printStackTrace()
                    }
                } catch (e : Exception) {
                    e.printStackTrace()
                }
            }
        }

        try {
            Thread.sleep(10000L)

            while (true) {
                logger.info("Wait for next event...")

                val values = queue.poll(60, TimeUnit.SECONDS)

                if (values != null) {
                    while (!queue.isEmpty()) {
                        queue.remove()
                    }

                    reconcile()
                }

                Thread.sleep(5000L)
            }
        } catch (e: Exception) {
            logger.error("An error occurred while processing the resources", e)
        }
    }

    private fun reconcile() {
        sharedLock.acquire()

        try {
            logger.info("Found ${clusters.size} Flink Cluster resource${if (clusters.size == 1) "" else "s"}")

            val divergentClusters = mutableMapOf<ClusterDescriptor, ClusterConfig>()

            val clusterDiff = ClusterResourcesDiffEvaluator()

            clusters.values.forEach { cluster ->
                val clusterConfig = ClusterConfigBuilder(
                    cluster.metadata,
                    cluster.spec
                ).build()

                val jobmnagerService = services.get(clusterConfig.descriptor)
                val sidecarDeployment = deployments.get(clusterConfig.descriptor)
                val jobmanagerStatefulSet = jobmanagerStatefulSets.get(clusterConfig.descriptor)
                val taskmanagerStatefulSet = taskmanagerStatefulSets.get(clusterConfig.descriptor)
                val jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaims.get(clusterConfig.descriptor)
                val taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaims.get(clusterConfig.descriptor)

                val actualResources = ClusterResources(
                    jobmanagerService = jobmnagerService,
                    sidecarDeployment = sidecarDeployment,
                    jobmanagerStatefulSet = jobmanagerStatefulSet,
                    taskmanagerStatefulSet = taskmanagerStatefulSet,
                    jobmanagerPersistentVolumeClaim = jobmanagerPersistentVolumeClaim,
                    taskmanagerPersistentVolumeClaim = taskmanagerPersistentVolumeClaim
                )

                if (clusterDiff.hasDiverged(clusterConfig, actualResources)) {
                    val lastUpdated = status.get(clusterConfig.descriptor)

                    if (lastUpdated == null) {
                        logger.info("Cluster ${clusterConfig.descriptor.name} has diverged. Reconciling state...")

                        divergentClusters.put(clusterConfig.descriptor, clusterConfig)

                        status.put(clusterConfig.descriptor, System.currentTimeMillis())
                    } else if (System.currentTimeMillis() - lastUpdated > 120000) {
                        logger.info("Cluster ${clusterConfig.descriptor.name} has diverged. Reconciling state...")

                        divergentClusters.put(clusterConfig.descriptor, clusterConfig)

                        status.put(clusterConfig.descriptor, System.currentTimeMillis())
                    }
                }
            }

            clusters.values.forEach { cluster ->
                val clusterConfig = ClusterConfigBuilder(
                    cluster.metadata,
                    cluster.spec
                ).build()

                if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                    logger.info("Deleting cluster ${clusterConfig.descriptor.name}...")

                    ClusterDeleteHandler.execute(clusterConfig.descriptor)
                }
            }

            clusters.values.forEach { cluster ->
                val clusterConfig = ClusterConfigBuilder(
                    cluster.metadata,
                    cluster.spec
                ).build()

                if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                    logger.info("Creating cluster ${clusterConfig.descriptor.name}...")

                    ClusterCreateHandler.execute("flink-operator", clusterConfig)
                }
            }

            val clusterConfigs = mutableMapOf<ClusterDescriptor, ClusterConfig>()

            clusters.values.forEach { cluster ->
                val clusterConfig = ClusterConfigBuilder(
                    cluster.metadata,
                    cluster.spec
                ).build()

                clusterConfigs.put(clusterConfig.descriptor, clusterConfig)
            }

            deleteOrphans(
                services,
                clusterConfigs,
                deployments,
                jobmanagerStatefulSets,
                taskmanagerStatefulSets,
                jobmanagerPersistentVolumeClaims,
                taskmanagerPersistentVolumeClaims
            )
        } finally {
            sharedLock.release()
        }
    }

    private fun deleteOrphans(
        services: MutableMap<ClusterDescriptor, V1Service>,
        flinkClusters: MutableMap<ClusterDescriptor, ClusterConfig>,
        deployments: MutableMap<ClusterDescriptor, V1Deployment>,
        jobmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        taskmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        jobmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>,
        taskmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>
    ) {
        val pendingDeleteClusters = mutableSetOf<ClusterDescriptor>()

        services.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && flinkClusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        deployments.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && flinkClusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        jobmanagerStatefulSets.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && flinkClusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        taskmanagerStatefulSets.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && flinkClusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        jobmanagerPersistentVolumeClaims.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && flinkClusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        taskmanagerPersistentVolumeClaims.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && flinkClusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        pendingDeleteClusters.forEach {
            logger.info("Deleting orphan cluster ${it.name}...")
            ClusterDeleteHandler.execute(it)
        }
    }
}
