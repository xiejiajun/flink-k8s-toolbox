package com.nextbreakpoint.common

interface SidecarCommand<T> {
    fun run(portForward: Int?, useNodePort: Boolean, params: T)
}

