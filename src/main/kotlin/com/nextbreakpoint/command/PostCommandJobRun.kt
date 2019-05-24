package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobRunParams

class PostCommandJobRun : PostCommand<JobRunParams>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, runParams: JobRunParams) {
        super.run(apiParams, "/job/run", runParams)
    }
}

