package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ApiParams
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(params: ApiParams): WebClient
}
