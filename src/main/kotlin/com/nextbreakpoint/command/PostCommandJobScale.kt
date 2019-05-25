package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobScaleParams

class PostCommandJobScale : DefaultPostCommand<JobScaleParams>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: JobScaleParams) {
        super.run(apiParams, "/job/scale", body)
    }
}

