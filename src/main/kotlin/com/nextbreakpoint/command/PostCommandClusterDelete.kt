package com.nextbreakpoint.command

import com.nextbreakpoint.DefaultWebClientFactory
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.Descriptor

class PostCommandClusterDelete : PostCommand<Descriptor>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, descriptor: Descriptor) {
        super.run(apiParams, "/cluster/delete", descriptor)
    }
}

