package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobRunParams

class PostCommandJobRun : DefaultPostCommand<JobRunParams>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: JobRunParams) {
        super.run(apiParams, "/job/run", body)
    }
}

