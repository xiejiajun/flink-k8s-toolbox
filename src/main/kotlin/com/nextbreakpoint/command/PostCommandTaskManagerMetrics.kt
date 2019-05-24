package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.TaskManagerDescriptor

class PostCommandTaskManagerMetrics : PostCommand<TaskManagerDescriptor>(
    DefaultWebClientFactory
) {
    fun run(apiParams: ApiParams, taskManagerDescriptor: TaskManagerDescriptor) {
        super.run(apiParams, "/taskmanager/metrics", taskManagerDescriptor)
    }
}

