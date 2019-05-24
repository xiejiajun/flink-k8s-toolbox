package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ApiParams

open class PostCommand<T>(val factory: WebClientFactory) {
    fun run(apiParams: ApiParams, path: String, body: T) {
        try {
            val client = factory.create(apiParams)
            try {
                val response = client.post(path)
                    .putHeader("content-type", "application/json")
                    .rxSendJson(body)
                    .toBlocking()
                    .value()
                println(response.bodyAsString())
            } finally {
                client.close()
            }
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }
}

