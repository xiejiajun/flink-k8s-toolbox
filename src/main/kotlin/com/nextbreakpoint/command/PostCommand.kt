package com.nextbreakpoint.command

import com.nextbreakpoint.WebClientFactory
import com.nextbreakpoint.model.ApiParams

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

