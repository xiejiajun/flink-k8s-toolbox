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
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread

class RunOperator {
    companion object {
        val logger: Logger = Logger.getLogger(RunOperator::class.simpleName)

        val resourcesDiffEvaluator = ClusterStatusEvaluator()

        val objectApi = CustomObjectsApi()

        val coreApi = CoreV1Api()

        val appsApi = AppsV1Api()
    }

    private val operationQueue = LinkedBlockingQueue<() -> Unit>()

    private val status = mutableMapOf<ClusterDescriptor, Long>()

    private val flinkClusters = mutableMapOf<ClusterDescriptor, V1FlinkCluster>()
    private val jobmanagerServices = mutableMapOf<ClusterDescriptor, V1Service>()
    private val sidecarDeployments = mutableMapOf<ClusterDescriptor, V1Deployment>()
    private val jobmanagerStatefulSets = mutableMapOf<ClusterDescriptor, V1StatefulSet>()
    private val taskmanagerStatefulSets = mutableMapOf<ClusterDescriptor, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = mutableMapOf<ClusterDescriptor, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = mutableMapOf<ClusterDescriptor, V1PersistentVolumeClaim>()

    fun run(config: OperatorConfig) {
        try {
            RunController.logger.info("Launching operator...")

            startWatching(config)

            Thread.sleep(10000L)

            while (true) {
                logger.info("Wait for next event...")

                val operation = operationQueue.poll(60, TimeUnit.SECONDS)

                if (operation != null) {
                    operation()

                    while (!operationQueue.isEmpty()) {
                        val nextOperation = operationQueue.remove()

                        nextOperation()
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
        logger.info("Found ${flinkClusters.size} Flink Cluster resource${if (flinkClusters.size == 1) "" else "s"}")

        val actualClusterConfigs = convertToClusterConfigs(flinkClusters).map {
            clusterConfig -> clusterConfig.descriptor to clusterConfig
        }.toMap()

        val divergentClusters = mutableMapOf<ClusterDescriptor, ClusterConfig>()

        actualClusterConfigs.values.forEach { clusterConfig ->
            val jobmnagerService = jobmanagerServices.get(clusterConfig.descriptor)
            val sidecarDeployment = sidecarDeployments.get(clusterConfig.descriptor)
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
                val lastUpdated = status[clusterConfig.descriptor]

                if (lastUpdated == null) {
                    logger.info("Cluster ${clusterConfig.descriptor.name} has diverged. Reconciling state...")

                    divergentClusters.put(clusterConfig.descriptor, clusterConfig)

                    status[clusterConfig.descriptor] = System.currentTimeMillis()
                } else if (System.currentTimeMillis() - lastUpdated > 120000) {
                    logger.info("Cluster ${clusterConfig.descriptor.name} has diverged. Reconciling state...")

                    divergentClusters.put(clusterConfig.descriptor, clusterConfig)

                    status[clusterConfig.descriptor] = System.currentTimeMillis()
                }
            }
        }

        actualClusterConfigs.values.forEach { clusterConfig ->
            if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                logger.info("Deleting cluster ${clusterConfig.descriptor.name}...")

                ClusterDeleteHandler.execute(clusterConfig.descriptor)
            }
        }

        actualClusterConfigs.values.forEach { clusterConfig ->
            if (divergentClusters.containsKey(clusterConfig.descriptor)) {
                logger.info("Creating cluster ${clusterConfig.descriptor.name}...")

                ClusterCreateHandler.execute("flink-operator", clusterConfig)
            }
        }

        deleteOrphans(
            actualClusterConfigs,
            jobmanagerServices,
            sidecarDeployments,
            jobmanagerStatefulSets,
            taskmanagerStatefulSets,
            jobmanagerPersistentVolumeClaims,
            taskmanagerPersistentVolumeClaims
        )
    }

    private fun convertToClusterConfigs(clusterResources: Map<ClusterDescriptor, V1FlinkCluster>) =
        clusterResources.values.map { cluster ->
            ClusterConfigBuilder(
                cluster.metadata,
                cluster.spec
            ).build()
        }.toList()

    private fun deleteOrphans(
        clusterConfigs: Map<ClusterDescriptor, ClusterConfig>,
        jobmanagerServices: MutableMap<ClusterDescriptor, V1Service>,
        sidecarDeployments: MutableMap<ClusterDescriptor, V1Deployment>,
        jobmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        taskmanagerStatefulSets: MutableMap<ClusterDescriptor, V1StatefulSet>,
        jobmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>,
        taskmanagerPersistentVolumeClaims: MutableMap<ClusterDescriptor, V1PersistentVolumeClaim>
    ) {
        val pendingDeleteClusters = mutableSetOf<ClusterDescriptor>()

        jobmanagerServices.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusterConfigs[descriptor] == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        sidecarDeployments.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusterConfigs[descriptor] == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        jobmanagerStatefulSets.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusterConfigs[descriptor] == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        taskmanagerStatefulSets.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusterConfigs[descriptor] == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        jobmanagerPersistentVolumeClaims.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusterConfigs[descriptor] == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        taskmanagerPersistentVolumeClaims.forEach { (descriptor, _) ->
            if (!pendingDeleteClusters.contains(descriptor) && clusterConfigs[descriptor] == null) {
                pendingDeleteClusters.add(descriptor)
            }
        }

        pendingDeleteClusters.forEach {
            logger.info("Deleting orphan cluster ${it.name}...")
            ClusterDeleteHandler.execute(it)
        }
    }

    private fun startWatching(operatorConfig: OperatorConfig) {
        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                flinkClusters.put(descriptor, resource)
            }, { descriptor, _ ->
                flinkClusters.remove(descriptor)
            }, {
                it.spec.clusterName
            }, {
                it.spec.environment
            }) {
                namespace -> ResourceWatchFactory.createWatchFlickClusterResources(objectApi, namespace)
            }
        }

        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                jobmanagerServices.put(descriptor, resource)
            }, { descriptor, _ ->
                jobmanagerServices.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                namespace -> ResourceWatchFactory.createWatchServiceResources(coreApi, namespace)
            }
        }

        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                sidecarDeployments.put(descriptor, resource)
            }, { descriptor, _ ->
                sidecarDeployments.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                namespace -> ResourceWatchFactory.createWatchDeploymentResources(appsApi, namespace)
            }
        }

        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerStatefulSets.put(descriptor, resource) else taskmanagerStatefulSets.put(descriptor, resource)
            }, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerStatefulSets.remove(descriptor) else taskmanagerStatefulSets.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                namespace -> ResourceWatchFactory.createWatchStatefulSetResources(appsApi, namespace)
            }
        }

        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerPersistentVolumeClaims.put(descriptor, resource) else taskmanagerPersistentVolumeClaims.put(descriptor, resource)
            }, { descriptor, resource ->
                if (resource.metadata.labels.get("role").equals("jobmanager")) jobmanagerPersistentVolumeClaims.remove(descriptor) else taskmanagerPersistentVolumeClaims.remove(descriptor)
            }, {
                it.metadata.labels.get("cluster")
            }, {
                it.metadata.labels.get("environment")
            }) {
                namespace -> ResourceWatchFactory.createWatchPermanentVolumeClaimResources(coreApi, namespace)
            }
        }
    }

    private fun <T> watchResources(
        namespace: String,
        operationQueue: BlockingQueue<() -> Unit>,
        onUpdateResource: (ClusterDescriptor, T) -> Unit,
        onDeleteResource: (ClusterDescriptor, T) -> Unit,
        extractClusterName: (T) -> String?,
        extractEnvironment: (T) -> String?,
        createResourceWatch: (String) -> Watch<T>
    ) {
        while (true) {
            try {
                createResourceWatch(namespace).forEach { resource ->
                    val clusterName = extractClusterName(resource.`object`)
                    val environment = extractEnvironment(resource.`object`)
                    if (clusterName != null && environment != null) {
                        when (resource.type) {
                            "ADDED", "MODIFIED" -> operationQueue.add { onUpdateResource(
                                ClusterDescriptor(
                                    namespace = namespace,
                                    name = clusterName,
                                    environment = environment
                                ), resource.`object`
                            ) }
                            "DELETED" -> operationQueue.add { onDeleteResource(
                                ClusterDescriptor(
                                    namespace = namespace,
                                    name = clusterName,
                                    environment = environment
                                ), resource.`object`
                            ) }
                        }
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
