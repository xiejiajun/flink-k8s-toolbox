package com.nextbreakpoint.operator

import com.nextbreakpoint.common.TestFactory
import io.kubernetes.client.models.V1Deployment
import io.kubernetes.client.models.V1Service
import io.kubernetes.client.models.V1StatefulSet
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.platform.runner.JUnitPlatform
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito.*

@RunWith(JUnitPlatform::class)
class ClusterResourcesBuilderTest {
    private val factory = mock(ClusterResourcesFactory::class.java)

    private val jobmanagerService = mock(V1Service::class.java)
    private val sidecarDeployment = mock(V1Deployment::class.java)
    private val jobmanagerStatefulSet = mock(V1StatefulSet::class.java)
    private val taskmanagerStatefulSet = mock(V1StatefulSet::class.java)

    private val clusterConfig = TestFactory.aCluster()

    private val builder = ClusterResourcesBuilder(factory, "myself", clusterConfig)

    private fun <T> any(): T {
        Mockito.any<T>()
        return uninitialized()
    }

    private fun <T> eq(value : T): T {
        Mockito.eq<T>(value)
        return uninitialized()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> uninitialized(): T = null as T

    @BeforeEach
    fun setup() {
        `when`(factory.createJobManagerService(any(), any(), any())).thenReturn(jobmanagerService)
        `when`(factory.createSidecarDeployment(any(), any(), any())).thenReturn(sidecarDeployment)
        `when`(factory.createJobManagerStatefulSet(any(), any(), any())).thenReturn(jobmanagerStatefulSet)
        `when`(factory.createTaskManagerStatefulSet(any(), any(), any())).thenReturn(taskmanagerStatefulSet)
    }

    @Test
    fun `should invoke factory to create resources`() {
        val resources = builder.build()

        assertThat(resources).isNotNull()
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.sidecarDeployment).isEqualTo(sidecarDeployment)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
        assertThat(resources.jobmanagerPersistentVolumeClaim).isNull()
        assertThat(resources.taskmanagerPersistentVolumeClaim).isNull()

        verify(factory, times(1)).createJobManagerService(eq("myself"), eq(clusterConfig.descriptor), eq(clusterConfig.jobmanager.serviceMode))
        verify(factory, times(1)).createSidecarDeployment(eq("myself"), eq(clusterConfig.descriptor), eq(clusterConfig.sidecar))
        verify(factory, times(1)).createJobManagerStatefulSet(eq("myself"), eq(clusterConfig.descriptor), eq(clusterConfig.jobmanager))
        verify(factory, times(1)).createTaskManagerStatefulSet(eq("myself"), eq(clusterConfig.descriptor), eq(clusterConfig.taskmanager))
    }

    @Test
    fun `should copy resources and modify jobmanager service`() {
        val resources = builder.build().withJobManagerService(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jobmanagerService).isNull()
        assertThat(resources.sidecarDeployment).isEqualTo(sidecarDeployment)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
        assertThat(resources.jobmanagerPersistentVolumeClaim).isNull()
        assertThat(resources.taskmanagerPersistentVolumeClaim).isNull()
    }

    @Test
    fun `should copy resources and modify sidecar deployment`() {
        val resources = builder.build().withSidecarDeployment(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.sidecarDeployment).isNull()
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
        assertThat(resources.jobmanagerPersistentVolumeClaim).isNull()
        assertThat(resources.taskmanagerPersistentVolumeClaim).isNull()
    }

    @Test
    fun `should copy resources and modify jobmanager statefulset`() {
        val resources = builder.build().withJobManagerStatefulSet(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.sidecarDeployment).isEqualTo(sidecarDeployment)
        assertThat(resources.jobmanagerStatefulSet).isNull()
        assertThat(resources.taskmanagerStatefulSet).isEqualTo(taskmanagerStatefulSet)
        assertThat(resources.jobmanagerPersistentVolumeClaim).isNull()
        assertThat(resources.taskmanagerPersistentVolumeClaim).isNull()
    }

    @Test
    fun `should copy resources and modify taskmanager statefulset`() {
        val resources = builder.build().withTaskManagerStatefulSet(null)

        assertThat(resources).isNotNull()
        assertThat(resources.jobmanagerService).isEqualTo(jobmanagerService)
        assertThat(resources.sidecarDeployment).isEqualTo(sidecarDeployment)
        assertThat(resources.jobmanagerStatefulSet).isEqualTo(jobmanagerStatefulSet)
        assertThat(resources.taskmanagerStatefulSet).isNull()
        assertThat(resources.jobmanagerPersistentVolumeClaim).isNull()
        assertThat(resources.taskmanagerPersistentVolumeClaim).isNull()
    }
}