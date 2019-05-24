package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobDescriptor

class PostCommandJobMetrics : PostCommand<JobDescriptor>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, jobDescriptor: JobDescriptor) {
        super.run(apiParams, "/job/metrics", jobDescriptor)
    }
}

