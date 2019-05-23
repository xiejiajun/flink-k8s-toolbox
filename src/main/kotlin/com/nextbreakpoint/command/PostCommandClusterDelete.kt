package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.model.Descriptor

class PostCommandClusterDelete : PostCommand<Descriptor>() {
    fun run(apiParams: ApiParams, descriptor: Descriptor) {
        super.run(apiParams, "/cluster/delete", descriptor)
    }
}

