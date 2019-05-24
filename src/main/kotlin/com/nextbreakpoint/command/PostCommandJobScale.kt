package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.handler.model.JobScaleParams

class PostCommandJobScale : PostCommand<JobScaleParams>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, scaleParams: JobScaleParams) {
        super.run(apiParams, "/job/scale", scaleParams)
    }
}

