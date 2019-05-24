package com.nextbreakpoint.command

import com.nextbreakpoint.common.DefaultWebClientFactory
import com.nextbreakpoint.common.PostCommand
import com.nextbreakpoint.common.model.ApiParams
import com.nextbreakpoint.operator.model.Cluster

class PostCommandClusterCreate : PostCommand<Cluster>(DefaultWebClientFactory) {
    fun run(apiParams: ApiParams, cluster: Cluster) {
        super.run(apiParams, "/cluster/create", cluster)
    }
}

