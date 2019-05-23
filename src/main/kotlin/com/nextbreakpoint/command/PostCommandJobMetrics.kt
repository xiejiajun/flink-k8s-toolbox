package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobDescriptor

class PostCommandJobMetrics : PostCommand<JobDescriptor>() {
    fun run(apiParams: ApiParams, jobDescriptor: JobDescriptor) {
        super.run(apiParams, "/job/metrics", jobDescriptor)
    }
}

