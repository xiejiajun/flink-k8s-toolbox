package com.nextbreakpoint.handler

import com.nextbreakpoint.operator.model.ClusterConfig
import com.nextbreakpoint.operator.ClusterResourcesBuilder
import com.nextbreakpoint.operator.DefaultClusterResourcesFactory
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import org.apache.log4j.Logger

object ClusterCreateHandler {
    private val logger = Logger.getLogger(ClusterCreateHandler::class.simpleName)

    fun execute(owner: String, clusterConfig: ClusterConfig): String {
        try {
            val appsApi = AppsV1Api()

            val coreApi = CoreV1Api()

            val statefulSets = appsApi.listNamespacedStatefulSet(
                clusterConfig.descriptor.namespace,
                null,
                null,
                null,
                null,
                "cluster=${clusterConfig.descriptor.name},environment=${clusterConfig.descriptor.environment}",
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
                clusterConfig
            ).build()

            logger.info("Creating Flink Service ...")

            val jobmanagerServiceOut = coreApi.createNamespacedService(
                clusterConfig.descriptor.namespace,
                jobmanagerService,
                null,
                null,
                null
            )

            logger.info("Service created ${jobmanagerServiceOut.metadata.name}")

            logger.info("Creating JobManager StatefulSet ...")

            val jobmanagerStatefulSetOut = appsApi.createNamespacedStatefulSet(
                clusterConfig.descriptor.namespace,
                jobmanagerStatefulSet,
                null,
                null,
                null
            )

            logger.info("StatefulSet created ${jobmanagerStatefulSetOut.metadata.name}")

            logger.info("Creating Sidecar Deployment ...")

            val sidecarDeploymentOut = appsApi.createNamespacedDeployment(
                clusterConfig.descriptor.namespace,
                sidecarDeployment,
                null,
                null,
                null
            )

            logger.info("Deployment created ${sidecarDeploymentOut.metadata.name}")

            logger.info("Creating TaskManager StatefulSet ...")

            val taskmanagerStatefulSetOut = appsApi.createNamespacedStatefulSet(
                clusterConfig.descriptor.namespace,
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