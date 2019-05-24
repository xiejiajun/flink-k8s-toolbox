package com.nextbreakpoint

import com.nextbreakpoint.model.ApiParams
import io.vertx.rxjava.ext.web.client.WebClient

interface WebClientFactory {
    fun create(params: ApiParams): WebClient
}
