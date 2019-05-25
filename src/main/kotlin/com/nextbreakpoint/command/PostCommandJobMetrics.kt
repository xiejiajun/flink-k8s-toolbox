package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobDescriptor

class PostCommandJobMetrics : DefaultPostCommand<JobDescriptor>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: JobDescriptor) {
        super.run(apiParams, "/job/metrics", body)
    }
}

