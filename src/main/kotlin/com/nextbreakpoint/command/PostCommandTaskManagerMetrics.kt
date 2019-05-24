package com.nextbreakpoint.command

import com.nextbreakpoint.DefaultWebClientFactory
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.TaskManagerDescriptor

class PostCommandTaskManagerMetrics : PostCommand<TaskManagerDescriptor>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, taskManagerDescriptor: TaskManagerDescriptor) {
        super.run(apiParams, "/taskmanager/metrics", taskManagerDescriptor)
    }
}

