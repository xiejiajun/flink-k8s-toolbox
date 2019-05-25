package com.nextbreakpoint.common

import com.nextbreakpoint.command.*

object DefaultCommandFactory : CommandFactory {
    override fun createRunControllerCommand() = RunController()

    override fun createRunOperatorCommand() = RunOperator()

    override fun createSidecarSubmitCommand() = RunSidecarSubmit()

    override fun createSidecarWatchCommand() = RunSidecarWatch()

    override fun createCreateClusterCommand() = PostCommandClusterCreate()

    override fun createDeleteClusterCommand() = PostCommandClusterDelete()

    override fun createRunJobCommand() = PostCommandJobRun()

    override fun createScaleJobCommand() = PostCommandJobScale()

    override fun createCancelJobCommand() = PostCommandJobCancel()

    override fun createGetJobDetailsCommand() = PostCommandJobDetails()

    override fun createGetJobMetricsCommand() = PostCommandJobMetrics()

    override fun createListJobsCommand() = PostCommandJobsList()

    override fun createGetJobManagerMetricsCommand() = PostCommandJobManagerMetrics()

    override fun createGetTaskManagerDetailsCommand() = PostCommandTaskManagerDetails()

    override fun createGetTaskManagerMetricsCommand() = PostCommandTaskManagerMetrics()

    override fun createListTaskManagersCommand() = PostCommandTaskManagersList()
}