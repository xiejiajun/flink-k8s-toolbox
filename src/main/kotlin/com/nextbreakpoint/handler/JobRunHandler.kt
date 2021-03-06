package com.nextbreakpoint.handler

import com.nextbreakpoint.model.ClusterDescriptor
import com.nextbreakpoint.model.JobRunParams
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.custom.Quantity
import io.kubernetes.client.models.*
import org.apache.log4j.Logger

object JobRunHandler {
    private val logger = Logger.getLogger(JobRunHandler::class.simpleName)

    fun execute(owner: String, runParams: JobRunParams): String {
        try {
            val api = AppsV1Api()

            deleteDeployment(api, runParams.descriptor)

            val environmentEnvVar = createEnvVar(
                "FLINK_ENVIRONMENT",
                runParams.descriptor.environment
            )

            val podNameEnvVar =
                createEnvVarFromField("POD_NAME", "metadata.name")

            val podNamespaceEnvVar = createEnvVarFromField(
                "POD_NAMESPACE",
                "metadata.namespace"
            )

            val arguments = mutableListOf<String>()

            if (runParams.sidecar.jarPath != null) {
                arguments.addAll(listOf(
                    "sidecar",
                    "submit",
                    "--namespace=${runParams.descriptor.namespace}",
                    "--environment=${runParams.descriptor.environment}",
                    "--cluster-name=${runParams.descriptor.name}",
                    "--jar-path=${runParams.sidecar.jarPath}",
                    "--parallelism=${runParams.sidecar.parallelism}"
                ))

                if (runParams.sidecar.className != null) {
                    arguments.add("--class-name=${runParams.sidecar.className}")
                }

                if (runParams.sidecar.savepoint != null) {
                    arguments.add("--savepoint=${runParams.sidecar.savepoint}")
                }

                runParams.sidecar.arguments?.split(" ")?.forEach { argument ->
                    arguments.add("--argument=$argument")
                }
            } else {
                arguments.addAll(listOf(
                    "sidecar",
                    "watch",
                    "--namespace=${runParams.descriptor.namespace}",
                    "--environment=${runParams.descriptor.environment}",
                    "--cluster-name=${runParams.descriptor.name}"
                ))
            }

            val componentLabel = Pair("component", "flink")

            val environmentLabel = Pair("environment", runParams.descriptor.environment)

            val clusterLabel = Pair("cluster", runParams.descriptor.name)

            val ownerLabel = Pair("owner", owner)

            val jobmanagerLabel = Pair("role", "jobmanager")

            val taskmanagerLabel = Pair("role", "taskmanager")

            val jobmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, jobmanagerLabel, environmentLabel)

            val taskmanagerLabels = mapOf(ownerLabel, clusterLabel, componentLabel, taskmanagerLabel, environmentLabel)

            val sidecarLabels = mapOf(ownerLabel, clusterLabel, componentLabel, environmentLabel)

            val jobmanagerSelector = V1LabelSelector().matchLabels(jobmanagerLabels)

            val taskmanagerSelector = V1LabelSelector().matchLabels(taskmanagerLabels)

            val sidecarSelector = V1LabelSelector().matchLabels(sidecarLabels)

            val jobmanagerAffinity =
                createAffinity(jobmanagerSelector, taskmanagerSelector)

            val sidecar = V1Container()
                .image(runParams.sidecar.image)
                .imagePullPolicy(runParams.sidecar.pullPolicy)
                .name("flink-sidecar")
                .args(arguments)
                .env(
                    listOf(
                        podNameEnvVar,
                        podNamespaceEnvVar,
                        environmentEnvVar
                    )
                )
                .resources(createSidecarResourceRequirements())

            val sidecarPodSpec = V1PodSpec()
                .containers(
                    listOf(sidecar)
                )
                .serviceAccountName(runParams.sidecar.serviceAccount)
                .imagePullSecrets(
                    listOf(
                        V1LocalObjectReference().name(runParams.sidecar.pullSecrets)
                    )
                )
                .affinity(jobmanagerAffinity)

            val sidecarMetadata =
                createObjectMeta("flink-sidecar-", sidecarLabels)

            val sidecarDeployment = V1Deployment()
                .metadata(sidecarMetadata)
                .spec(
                    V1DeploymentSpec()
                        .replicas(1)
                        .template(
                            V1PodTemplateSpec()
                                .spec(sidecarPodSpec)
                                .metadata(sidecarMetadata)
                        )
                        .selector(sidecarSelector)
                )

            logger.info("Creating Sidecar Deployment ...")

            val sidecarDeploymentOut = api.createNamespacedDeployment(
                runParams.descriptor.namespace,
                sidecarDeployment,
                null,
                null,
                null
            )

            logger.info("Deployment created ${sidecarDeploymentOut.metadata.name}")

            return "{\"status\":\"SUCCESS\"}"
        } catch (e : Exception) {
            throw RuntimeException(e)
        }
    }

    private fun createEnvVarFromField(s: String, s1: String) = V1EnvVar()
        .name(s)
        .valueFrom(
            V1EnvVarSource()
                .fieldRef(
                    V1ObjectFieldSelector().fieldPath(s1)
                )
        )

    private fun createEnvVar(name: String, value: String) = V1EnvVar()
        .name(name)
        .value(value)

    private fun createObjectMeta(name: String, jobmanagerLabels: Map<String, String>) = V1ObjectMeta()
        .generateName(name)
        .labels(jobmanagerLabels)

    private fun createAffinity(jobmanagerSelector: V1LabelSelector?, taskmanagerSelector: V1LabelSelector?) = V1Affinity()
        .podAntiAffinity(
            V1PodAntiAffinity().preferredDuringSchedulingIgnoredDuringExecution(
                listOf(
                    V1WeightedPodAffinityTerm().weight(50).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(jobmanagerSelector)
                    ),
                    V1WeightedPodAffinityTerm().weight(100).podAffinityTerm(
                        V1PodAffinityTerm()
                            .topologyKey("kubernetes.io/hostname")
                            .labelSelector(taskmanagerSelector)
                    )
                )
            )
        )

    private fun createSidecarResourceRequirements() = V1ResourceRequirements()
        .limits(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("200Mi")
            )
        )
        .requests(
            mapOf(
                "cpu" to Quantity("0.2"),
                "memory" to Quantity("200Mi")
            )
        )

    private fun deleteDeployment(api: AppsV1Api, descriptor: ClusterDescriptor) {
        val deployments = api.listNamespacedDeployment(
            descriptor.namespace,
            null,
            null,
            null,
            null,
            "cluster=${descriptor.name},environment=${descriptor.environment}",
            null,
            null,
            30,
            null
        )

        deployments.items.forEach { deployment ->
            try {
                logger.info("Removing Deployment ${deployment.metadata.name}...")

                val status = api.deleteNamespacedDeployment(
                    deployment.metadata.name,
                    descriptor.namespace,
                    V1DeleteOptions(),
                    "true",
                    null,
                    null,
                    null,
                    null
                )

                logger.info("Response status: ${status.reason}")

                status.details.causes.forEach { logger.info(it.message) }
            } catch (e: Exception) {
                // ignore. see bug https://github.com/kubernetes/kubernetes/issues/59501
            }
        }
    }
}