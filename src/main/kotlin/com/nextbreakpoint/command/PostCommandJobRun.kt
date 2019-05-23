package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobRunParams

class PostCommandJobRun : PostCommand<JobRunParams>() {
    fun run(apiParams: ApiParams, runParams: JobRunParams) {
        super.run(apiParams, "/job/run", runParams)
    }
}

