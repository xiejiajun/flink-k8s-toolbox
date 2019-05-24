package com.nextbreakpoint

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams

object DefaultWebClientFactory : WebClientFactory {
    override fun create(params: ApiParams) = createWebClient(host = params.host, port = params.port)
}