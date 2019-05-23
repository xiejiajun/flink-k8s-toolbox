package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.JobScaleParams

class PostCommandJobScale : PostCommand<JobScaleParams>() {
    fun run(apiParams: ApiParams, scaleParams: JobScaleParams) {
        super.run(apiParams, "/job/scale", scaleParams)
    }
}

