package com.nextbreakpoint.operator.model

import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1PersistentVolumeClaim
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet

data class ClusterResourcesDiff(
    val jobmanagerService: V1Service?,
    val sidecarDeployment: V1Deployment?,
    val jobmanagerStatefulSet: V1StatefulSet?,
    val taskmanagerStatefulSet: V1StatefulSet?,
    val jobmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?,
    val taskmanagerPersistentVolumeClaim: V1PersistentVolumeClaim?
)

