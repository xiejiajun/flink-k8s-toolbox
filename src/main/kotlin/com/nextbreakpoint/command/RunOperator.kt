package com.nextbreakpoint.command

import com.nextbreakpoint.handler.ClusterCreateHandler
import com.nextbreakpoint.handler.ClusterDeleteHandler
import com.nextbreakpoint.model.*
import com.nextbreakpoint.operator.*
import com.nextbreakpoint.operator.model.Cluster
import com.nextbreakpoint.operator.ClusterConfigBuilder
import com.nextbreakpoint.operator.model.ClusterResources
import com.nextbreakpoint.operator.model.ClusterStatus
import com.nextbreakpoint.operator.model.ResourceStatus
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

    private val status = mutableMapOf<Descriptor, Long>()

    private val flinkClusters = mutableMapOf<Descriptor, V1FlinkCluster>()
    private val jobmanagerServices = mutableMapOf<Descriptor, V1Service>()
    private val sidecarDeployments = mutableMapOf<Descriptor, V1Deployment>()
    private val jobmanagerStatefulSets = mutableMapOf<Descriptor, V1StatefulSet>()
    private val taskmanagerStatefulSets = mutableMapOf<Descriptor, V1StatefulSet>()
    private val jobmanagerPersistentVolumeClaims = mutableMapOf<Descriptor, V1PersistentVolumeClaim>()
    private val taskmanagerPersistentVolumeClaims = mutableMapOf<Descriptor, V1PersistentVolumeClaim>()

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

        val divergentClusters = mutableMapOf<Descriptor, Cluster>()

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

            val clusterStatus = resourcesDiffEvaluator.status(clusterConfig, actualResources)

            if (hasDiverged(clusterStatus)) {
                printStatus(clusterStatus)

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

                ClusterDeleteHandler.execute("flink-operator", clusterConfig.descriptor)
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

    private fun hasDiverged(clusterStatus: ClusterStatus): Boolean {
        if (clusterStatus.jobmanagerService.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterStatus.sidecarDeployment.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterStatus.jobmanagerStatefulSet.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterStatus.taskmanagerStatefulSet.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterStatus.jobmanagerPersistentVolumeClaim.first != ResourceStatus.VALID) {
            return true
        }

        if (clusterStatus.taskmanagerPersistentVolumeClaim.first != ResourceStatus.VALID) {
            return true
        }

        return false
    }

    private fun printStatus(clusterStatus: ClusterStatus) {
        clusterStatus.jobmanagerService.second.forEach { println("jobmanager service: ${it}") }

        clusterStatus.sidecarDeployment.second.forEach { println("sidecar deployment: ${it}") }

        clusterStatus.jobmanagerStatefulSet.second.forEach { println("jobmanager stateful set: ${it}") }

        clusterStatus.taskmanagerStatefulSet.second.forEach { println("taskmanager stateful set: ${it}") }

        clusterStatus.jobmanagerPersistentVolumeClaim.second.forEach { println("jobmanager persistent volume claim: ${it}") }

        clusterStatus.taskmanagerPersistentVolumeClaim.second.forEach { println("taskmanager persistent volume claim: ${it}") }
    }

    private fun convertToClusterConfigs(resources: Map<Descriptor, V1FlinkCluster>) =
        resources.values.map { cluster ->
            ClusterConfigBuilder(
                cluster.metadata,
                cluster.spec
            ).build()
        }.toList()

    private fun deleteOrphans(
        configs: Map<Descriptor, Cluster>,
        jobmanagerServices: MutableMap<Descriptor, V1Service>,
        sidecarDeployments: MutableMap<Descriptor, V1Deployment>,
        jobmanagerStatefulSets: MutableMap<Descriptor, V1StatefulSet>,
        taskmanagerStatefulSets: MutableMap<Descriptor, V1StatefulSet>,
        jobmanagerPersistentVolumeClaims: MutableMap<Descriptor, V1PersistentVolumeClaim>,
        taskmanagerPersistentVolumeClaims: MutableMap<Descriptor, V1PersistentVolumeClaim>
    ) {
        val pendingDeleteClusters = mutableSetOf<Descriptor>()

        pendingDeleteClusters.addAll(jobmanagerServices.filter { (descriptor, _) -> configs[descriptor] == null }.keys)
        pendingDeleteClusters.addAll(sidecarDeployments.filter { (descriptor, _) -> configs[descriptor] == null }.keys)
        pendingDeleteClusters.addAll(jobmanagerStatefulSets.filter { (descriptor, _) -> configs[descriptor] == null }.keys)
        pendingDeleteClusters.addAll(taskmanagerStatefulSets.filter { (descriptor, _) -> configs[descriptor] == null }.keys)
        pendingDeleteClusters.addAll(jobmanagerPersistentVolumeClaims.filter { (descriptor, _) -> configs[descriptor] == null }.keys)
        pendingDeleteClusters.addAll(taskmanagerPersistentVolumeClaims.filter { (descriptor, _) -> configs[descriptor] == null }.keys)

        pendingDeleteClusters.forEach {
            logger.info("Deleting orphan cluster ${it.name}...")
            ClusterDeleteHandler.execute("flink-operator", it)
        }
    }

    private fun startWatching(operatorConfig: OperatorConfig) {
        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                flinkClusters.put(descriptor, resource)
            }, { descriptor, _ ->
                flinkClusters.remove(descriptor)
            }, {
                it.metadata.name
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
            }) {
                namespace -> ResourceWatchFactory.createWatchDeploymentResources(appsApi, namespace)
            }
        }

        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                when {
                    resource.metadata.labels.get("role") == "jobmanager" ->
                        jobmanagerStatefulSets.put(descriptor, resource)

                    resource.metadata.labels.get("role") == "taskmanager" ->
                        taskmanagerStatefulSets.put(descriptor, resource)
                }
            }, { descriptor, resource ->
                when {
                    resource.metadata.labels.get("role") == "jobmanager" ->
                        jobmanagerStatefulSets.remove(descriptor)

                    resource.metadata.labels.get("role") == "taskmanager" ->
                        taskmanagerStatefulSets.remove(descriptor)
                }
            }, {
                it.metadata.labels.get("cluster")
            }) {
                namespace -> ResourceWatchFactory.createWatchStatefulSetResources(appsApi, namespace)
            }
        }

        thread {
            watchResources(operatorConfig.namespace, operationQueue, { descriptor, resource ->
                when {
                    resource.metadata.labels.get("role") == "jobmanager" ->
                        jobmanagerPersistentVolumeClaims.put(descriptor, resource)

                    resource.metadata.labels.get("role") == "taskmanager" ->
                        taskmanagerPersistentVolumeClaims.put(descriptor, resource)
                }
            }, { descriptor, resource ->
                when {
                    resource.metadata.labels.get("role") == "jobmanager" ->
                        jobmanagerPersistentVolumeClaims.remove(descriptor)

                    resource.metadata.labels.get("role") == "taskmanager" ->
                        taskmanagerPersistentVolumeClaims.remove(descriptor)
                }
            }, {
                it.metadata.labels.get("cluster")
            }) {
                namespace -> ResourceWatchFactory.createWatchPermanentVolumeClaimResources(coreApi, namespace)
            }
        }
    }

    private fun <T> watchResources(
        namespace: String,
        operationQueue: BlockingQueue<() -> Unit>,
        onUpdateResource: (Descriptor, T) -> Unit,
        onDeleteResource: (Descriptor, T) -> Unit,
        extractClusterName: (T) -> String?,
        createResourceWatch: (String) -> Watch<T>
    ) {
        while (true) {
            try {
                createResourceWatch(namespace).forEach { resource ->
                    val clusterName = extractClusterName(resource.`object`)
                    if (clusterName != null) {
                        when (resource.type) {
                            "ADDED", "MODIFIED" -> operationQueue.add { onUpdateResource(
                                Descriptor(
                                    namespace = namespace,
                                    name = clusterName
                                ), resource.`object`
                            ) }
                            "DELETED" -> operationQueue.add { onDeleteResource(
                                Descriptor(
                                    namespace = namespace,
                                    name = clusterName
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
