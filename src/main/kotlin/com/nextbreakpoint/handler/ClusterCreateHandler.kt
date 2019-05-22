package com.nextbreakpoint.handler

import com.nextbreakpoint.operator.model.Cluster
import com.nextbreakpoint.operator.ClusterResourcesBuilder
import com.nextbreakpoint.operator.DefaultClusterResourcesFactory
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

object ClusterCreateHandler {
    private val logger = Logger.getLogger(ClusterCreateHandler::class.simpleName)

    fun execute(owner: String, cluster: Cluster): String {
        try {
            val appsApi = AppsV1Api()

            val coreApi = CoreV1Api()

            val statefulSets = appsApi.listNamespacedStatefulSet(
                cluster.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${cluster.descriptor.name},environment=${cluster.descriptor.environment}",
                null,
                null,
                30,
                null
            )

            if (statefulSets.items.size > 0) {
                throw RuntimeException("Cluster already exists")
            }

            val (jobmanagerService, sidecarDeployment, jobmanagerStatefulSet, taskmanagerStatefulSet) = ClusterResourcesBuilder(
                DefaultClusterResourcesFactory,
                owner,
                cluster
            ).build()

            logger.info("Creating Flink Service ...")

            val jobmanagerServiceOut = coreApi.createNamespacedService(
                cluster.descriptor.namespace,
                jobmanagerService,
                null,
                null,
                null
            )

            logger.info("Service created ${jobmanagerServiceOut.metadata.name}")

            logger.info("Creating JobManager StatefulSet ...")

            val jobmanagerStatefulSetOut = appsApi.createNamespacedStatefulSet(
                cluster.descriptor.namespace,
                jobmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("StatefulSet created ${jobmanagerStatefulSetOut.metadata.name}")

            logger.info("Creating Sidecar Deployment ...")

            val sidecarDeploymentOut = appsApi.createNamespacedDeployment(
                cluster.descriptor.namespace,
                sidecarDeployment,
                null,
                null,
                null
            )

            logger.info("Deployment created ${sidecarDeploymentOut.metadata.name}")

            logger.info("Creating TaskManager StatefulSet ...")

            val taskmanagerStatefulSetOut = appsApi.createNamespacedStatefulSet(
                cluster.descriptor.namespace,
                taskmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("StatefulSet created ${taskmanagerStatefulSetOut.metadata.name}")

            return "{\"status\":\"SUCCESS\"}"
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }
}