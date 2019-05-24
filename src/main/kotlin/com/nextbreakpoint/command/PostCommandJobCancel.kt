package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobCancelParams

class PostCommandJobCancel : PostCommand<JobCancelParams>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, cancelParams: JobCancelParams) {
        super.run(apiParams, "/job/cancel", cancelParams)
    }
}

