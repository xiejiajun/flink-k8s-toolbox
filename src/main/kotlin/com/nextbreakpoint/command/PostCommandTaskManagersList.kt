package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.common.model.Descriptor

class PostCommandTaskManagersList : PostCommand<Descriptor>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, descriptor: Descriptor) {
        super.run(apiParams, "/taskmanagers/list", descriptor)
    }
}

