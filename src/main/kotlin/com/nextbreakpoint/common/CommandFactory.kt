package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.Descriptor
import com.nextbreakpoint.handler.model.*
import com.nextbreakpoint.operator.model.Cluster

interface CommandFactory {
    fun createRunControllerCommand() : ServerCommand<ControllerConfig>

    fun createRunOperatorCommand() : ServerCommand<OperatorConfig>

    fun createSidecarSubmitCommand() : SidecarCommand<JobSubmitParams>

    fun createSidecarWatchCommand() : SidecarCommand<WatchParams>

    fun createCreateClusterCommand() : PostCommand<Cluster>

    fun createDeleteClusterCommand() : PostCommand<Descriptor>

    fun createRunJobCommand() : PostCommand<JobRunParams>

    fun createScaleJobCommand() : PostCommand<JobScaleParams>

    fun createCancelJobCommand() : PostCommand<JobCancelParams>

    fun createGetJobDetailsCommand() : PostCommand<JobDescriptor>

    fun createGetJobMetricsCommand() : PostCommand<JobDescriptor>

    fun createListJobsCommand() : PostCommand<JobsListParams>

    fun createGetJobManagerMetricsCommand() : PostCommand<Descriptor>

    fun createGetTaskManagerDetailsCommand() : PostCommand<TaskManagerDescriptor>

    fun createGetTaskManagerMetricsCommand() : PostCommand<TaskManagerDescriptor>

    fun createListTaskManagersCommand() : PostCommand<Descriptor>
}