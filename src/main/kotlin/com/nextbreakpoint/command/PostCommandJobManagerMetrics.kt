package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.common.model.Descriptor

class PostCommandJobManagerMetrics : DefaultPostCommand<Descriptor>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: Descriptor) {
        super.run(apiParams, "/jobmanager/metrics", body)
    }
}

