package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.DefaultPostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.operator.model.Cluster

class PostCommandClusterCreate : DefaultPostCommand<Cluster>(DefaultWebClientFactory) {
    override fun run(apiParams: ApiParams, body: Cluster) {
        super.run(apiParams, "/cluster/create", body)
    }
}

