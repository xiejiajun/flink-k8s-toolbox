package com.nextbreakpoint.common

import com.nextbreakpoint.common.CommandUtils.createWebClient
import com.nextbreakpoint.common.model.ApiParams

object DefaultWebClientFactory : WebClientFactory {
    override fun create(params: ApiParams) = createWebClient(host = params.host, port = params.port)
}