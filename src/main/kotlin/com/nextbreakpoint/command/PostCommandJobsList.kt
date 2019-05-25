package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobsListParams

class PostCommandJobsList : DefaultPostCommand<JobsListParams>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: JobsListParams) {
        super.run(apiParams, "/jobs/list", body)
    }
}

