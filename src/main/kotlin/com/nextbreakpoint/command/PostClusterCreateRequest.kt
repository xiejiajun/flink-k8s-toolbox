package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams
import com.nextbreakpoint.operator.model.Cluster

class PostClusterCreateRequest {
    fun run(apiParams: ApiParams, cluster: Cluster) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
        try {
            val response = client.post("/cluster/create")
                .putHeader("content-type", "application/json")
                .rxSendJson(cluster)
                .toBlocking()
                .value()
            println(response.bodyAsString())
        } catch (e: Exception) {
            throw RuntimeException(e)
        } finally {
            client.close()
        }
    }
}

