package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobsListParams

class PostCommandJobsList : PostCommand<JobsListParams>() {
    fun run(apiParams: ApiParams, listParams: JobsListParams) {
        super.run(apiParams, "/jobs/list", listParams)
    }
}

