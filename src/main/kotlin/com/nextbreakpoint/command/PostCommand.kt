package com.nextbreakpoint.command

import com.nextbreakpoint.CommandUtils.createWebClient
import com.nextbreakpoint.model.ApiParams

open class PostCommand<T> {
    fun run(apiParams: ApiParams, path: String, body: T) {
        val client = createWebClient(host = apiParams.host, port = apiParams.port)
        try {
            val response = client.post(path)
                .putHeader("content-type", "application/json")
                .rxSendJson(body)
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

