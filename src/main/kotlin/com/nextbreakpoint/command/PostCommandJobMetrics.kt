package com.nextbreakpoint.command

import com.nextbreakpoint.DefaultWebClientFactory
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobDescriptor

class PostCommandJobMetrics : PostCommand<JobDescriptor>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, jobDescriptor: JobDescriptor) {
        super.run(apiParams, "/job/metrics", jobDescriptor)
    }
}
