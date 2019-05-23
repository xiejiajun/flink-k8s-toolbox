package com.nextbreakpoint.command

import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.operator.model.Cluster

class PostCommandClusterCreate : PostCommand<Cluster>() {
    fun run(apiParams: ApiParams, cluster: Cluster) {
        super.run(apiParams, "/cluster/create", cluster)
    }
}

