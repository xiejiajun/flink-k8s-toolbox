package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobsListParams

class PostCommandJobsList : PostCommand<JobsListParams>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, listParams: JobsListParams) {
        super.run(apiParams, "/jobs/list", listParams)
    }
}

