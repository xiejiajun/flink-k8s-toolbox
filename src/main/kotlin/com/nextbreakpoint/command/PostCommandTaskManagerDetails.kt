package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.TaskManagerDescriptor

class PostCommandTaskManagerDetails : PostCommand<TaskManagerDescriptor>() {
    fun run(apiParams: ApiParams, taskManagerDescriptor: TaskManagerDescriptor) {
        super.run(apiParams, "/taskmanager/details", taskManagerDescriptor)
    }
}

