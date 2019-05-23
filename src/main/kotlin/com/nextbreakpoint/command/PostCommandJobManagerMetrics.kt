package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.Descriptor

class PostCommandJobManagerMetrics : PostCommand<Descriptor>() {
    fun run(apiParams: ApiParams, descriptor: Descriptor) {
        super.run(apiParams, "/jobmanager/metrics", descriptor)
    }
}

