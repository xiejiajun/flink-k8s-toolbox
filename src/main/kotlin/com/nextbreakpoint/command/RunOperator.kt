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
import io.kubernetes.client.util.Watch
import org.apache.log4j.Logger
import java.net.SocketTimeoutException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class RunOperator {
    companion object {
        val logger: Logger = Logger.getLogger(RunOperator::class.simpleName)
    }

    private val sharedLock = Semaphore(1)
    private val queue = LinkedBlockingQueue<Any>()
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

        try {
            startWatching(config)

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

            val resourcesDiffEvaluator = ClusterResourcesDiffEvaluator()

            val actualClusterConfigs = clusters.values.map { cluster ->
                ClusterConfigBuilder(
                    cluster.metadata,
                    cluster.spec
                ).build()
            }.toList()

            val divergentClusters = mutableMapOf<ClusterDescriptor, ClusterConfig>()

            actualClusterConfigs.forEach { clusterConfig ->
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

                if (resourcesDiffEvaluator.hasDiverged(clusterConfig, actualResources)) {
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

            actualClusterConfigs.forEach { clusterConfig ->
                if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                    logger.info("Deleting cluster ${clusterConfig.descriptor.name}...")

                    ClusterDeleteHandler.execute(clusterConfig.descriptor)
                }
            }

            actualClusterConfigs.forEach { clusterConfig ->
                if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                    logger.info("Creating cluster ${clusterConfig.descriptor.name}...")

                    ClusterCreateHandler.execute("flink-operator", clusterConfig)
                }
            }

            val clusterConfigs = actualClusterConfigs.map {
                clusterConfig -> clusterConfig.descriptor to clusterConfig
            }.toMap()

            deleteOrphans(
                clusterConfigs,
                services,
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
        clusters: Map<ClusterDescriptor, ClusterConfig>,
        services: MutableMap<ClusterDescriptor, V1Service>,
        deployments: MutableMap<ClusterDescriptor, V1Deployment>,
        jobmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        taskmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        jobmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>,
        taskmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>
    ) {
        val pendingDeleteClusters = mutableSetOf<ClusterDescriptor>()

        services.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        deployments.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        jobmanagerStatefulSets.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        taskmanagerStatefulSets.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        jobmanagerPersistentVolumeClaims.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        taskmanagerPersistentVolumeClaims.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusters.get(descriptor) == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        pendingDeleteClusters.forEach {
            logger.info("Deleting orphan cluster ${it.name}...")
            ClusterDeleteHandler.execute(it)
        }
    }

    private fun startWatching(config: OperatorConfig) {
        val objectApi = CustomObjectsApi()

        val coreApi = CoreV1Api()

        val appsApi = AppsV1Api()

        thread {
            watchResources(config, sharedLock, queue, { descriptor, resource ->
                clusters.put(descriptor, resource)
            }, { descriptor, _ ->
                clusters.remove(descriptor)
            }, {
                it.spec.clusterName
            }, {
                it.spec.environment
            }) {
                ClusterResourcesWatchFactory.createWatchFlickClusterResources(it.namespace, objectApi)
            }
        }

        thread {
            watchResources(config, sharedLock, queue, { descriptor, resource ->
                services.put(descriptor, resource)
            }, { descriptor, _ ->
                services.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                ClusterResourcesWatchFactory.createWatchServiceResources(it.namespace, coreApi)
            }
        }

        thread {
            watchResources(config, sharedLock, queue, { descriptor, resource ->
                deployments.put(descriptor, resource)
            }, { descriptor, _ ->
                deployments.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                ClusterResourcesWatchFactory.createWatchDeploymentResources(it.namespace, appsApi)
            }
        }

        thread {
            watchResources(config, sharedLock, queue, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerStatefulSets.put(descriptor, resource) else taskmanagerStatefulSets.put(descriptor, resource)
            }, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerStatefulSets.remove(descriptor) else taskmanagerStatefulSets.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                ClusterResourcesWatchFactory.createWatchStatefulSetResources(it.namespace, appsApi)
            }
        }

        thread {
            watchResources(config, sharedLock, queue, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerPersistentVolumeClaims.put(descriptor, resource) else taskmanagerPersistentVolumeClaims.put(descriptor, resource)
            }, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerPersistentVolumeClaims.remove(descriptor) else taskmanagerPersistentVolumeClaims.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                ClusterResourcesWatchFactory.createWatchPermanentVolumeClaimResources(it.namespace, coreApi)
            }
        }
    }

    private fun <T> watchResources(
        config: OperatorConfig,
        semaphore: Semaphore,
        notificationQueue: LinkedBlockingQueue<Any>,
        onUpdate: (ClusterDescriptor, T) -> Unit,
        onDelete: (ClusterDescriptor, T) -> Unit,
        extractClusterName: (T) -> String?,
        extractEnvironment: (T) -> String?,
        createWatch: (OperatorConfig) -> Watch<T>
    ) {
        while (true) {
            try {
                createWatch(config).forEach { resource ->
                    try {
                        semaphore.acquire()
                        val clusterName = extractClusterName(resource.`object`)
                        val environment = extractEnvironment(resource.`object`)
                        if (clusterName != null && environment != null) {
                            when (resource.type) {
                                "ADDED", "MODIFIED" -> onUpdate(
                                    ClusterDescriptor(
                                        namespace = config.namespace,
                                        name = clusterName,
                                        environment = environment
                                    ), resource.`object`
                                )
                                "DELETED" -> onDelete(
                                    ClusterDescriptor(
                                        namespace = config.namespace,
                                        name = clusterName,
                                        environment = environment
                                    ), resource.`object`
                                )
                            }
                            notificationQueue.add(resource.`object`)
                        }
                    } finally {
                        semaphore.release()
                    }
                }
            } catch (e: InterruptedException) {
                break
            } catch (e: RuntimeException) {
                if (!(e.cause is SocketTimeoutException)) {
                    e.printStackTrace()
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }
}
