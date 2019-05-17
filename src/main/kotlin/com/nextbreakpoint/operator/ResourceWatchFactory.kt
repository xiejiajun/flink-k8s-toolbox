package com.nextbreakpoint.operator

import com.google.gson.reflect.TypeToken
import com.nextbreakpoint.model.V1FlinkCluster
import io.kubernetes.client.Configuration
import io.kubernetes.client.apis.AppsV1Api
import io.kubernetes.client.apis.CoreV1Api
import io.kubernetes.client.apis.CustomObjectsApi
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import io.kubernetes.client.util.Watch

object ResourceWatchFactory {
    fun createWatchFlickClusterResources(objectApi: CustomObjectsApi, namespace: String): Watch<V1FlinkCluster> =
        Watch.createWatch<V1FlinkCluster>(
            Configuration.getDefaultApiClient(),
            objectApi.listNamespacedCustomObjectCall(
                "nextbreakpoint.com",
                "v1",
                namespace,
                "flinkclusters",
                null,
                null,
                null,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1FlinkCluster>>() {}.type
        )

    fun createWatchServiceResources(coreApi: CoreV1Api, namespace: String): Watch<V1Service> =
        Watch.createWatch<V1Service>(
            Configuration.getDefaultApiClient(),
            coreApi.listNamespacedServiceCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Service>>() {}.type
        )

    fun createWatchDeploymentResources(appsApi: AppsV1Api, namespace: String): Watch<V1Deployment> =
        Watch.createWatch<V1Deployment>(
            Configuration.getDefaultApiClient(),
            appsApi.listNamespacedDeploymentCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1Deployment>>() {}.type
        )

    fun createWatchStatefulSetResources(appsApi: AppsV1Api, namespace: String): Watch<V1StatefulSet> =
        Watch.createWatch<V1StatefulSet>(
            Configuration.getDefaultApiClient(),
            appsApi.listNamespacedStatefulSetCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1StatefulSet>>() {}.type
        )

    fun createWatchPermanentVolumeClaimResources(coreApi: CoreV1Api, namespace: String): Watch<V1PersistentVolumeClaim> =
        Watch.createWatch<V1PersistentVolumeClaim>(
            Configuration.getDefaultApiClient(),
            coreApi.listNamespacedPersistentVolumeClaimCall(
                namespace,
                null,
                null,
                null,
                null,
                "component=flink,owner=flink-operator",
                null,
                null,
                600,
                true,
                null,
                null
            ),
            object : TypeToken<Watch.Response<V1PersistentVolumeClaim>>() {}.type
        )
}