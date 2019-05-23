package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobCancelParams

class PostCommandJobCancel : PostCommand<JobCancelParams>() {
    fun run(apiParams: ApiParams, cancelParams: JobCancelParams) {
        super.run(apiParams, "/job/cancel", cancelParams)
    }
}

