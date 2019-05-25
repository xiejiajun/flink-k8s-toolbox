package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobCancelParams

class PostCommandJobCancel : DefaultPostCommand<JobCancelParams>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: JobCancelParams) {
        super.run(apiParams, "/job/cancel", body)
    }
}

