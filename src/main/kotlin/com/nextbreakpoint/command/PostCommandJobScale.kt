package com.nextbreakpoint.command

import com.nextbreakpoint.DefaultWebClientFactory
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobScaleParams

class PostCommandJobScale : PostCommand<JobScaleParams>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, scaleParams: JobScaleParams) {
        super.run(apiParams, "/job/scale", scaleParams)
    }
}

