package com.nextbreakpoint.common

import com.nextbreakpoint.common.model.ApiParams

interface PostCommand<T> {
    fun run(apiParams: ApiParams, body: T)
}

