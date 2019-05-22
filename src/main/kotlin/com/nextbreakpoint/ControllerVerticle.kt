package com.nextbreakpoint

import com.google.gson.Gson
import com.nextbreakpoint.CommandUtils.createKubernetesClient
import com.nextbreakpoint.handler.*
import com.nextbreakpoint.model.*
import com.nextbreakpoint.operator.model.Cluster
import io.kubernetes.client.Configuration
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.handler.LoggerFormat
import io.vertx.rxjava.core.AbstractVerticle
import io.vertx.rxjava.core.Future
import io.vertx.rxjava.core.http.HttpServer
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.RoutingContext
import io.vertx.rxjava.ext.web.handler.BodyHandler
import io.vertx.rxjava.ext.web.handler.LoggerHandler
import io.vertx.rxjava.ext.web.handler.TimeoutHandler
import rx.Completable
import rx.Single

class ControllerVerticle : AbstractVerticle() {
    override fun rxStart(): Completable {
        return createServer(vertx.orCreateContext.config()).toCompletable()
    }

    private fun makeError(error: Throwable) = "{\"status\":\"FAILURE\",\"error\":\"${error.message}\"}"

    private fun createServer(config: JsonObject): Single<HttpServer> {
        val port: Int = config.getInteger("port") ?: 4444

        val portForward: Int? = config.getInteger("portForward") ?: null

        val kubeConfig: String? = config.getString("kubeConfig") ?: null

        val useNodePort = kubeConfig != null

        val mainRouter = Router.router(vertx)

        mainRouter.route().handler(LoggerHandler.create(true, LoggerFormat.DEFAULT))
        mainRouter.route().handler(BodyHandler.create())
        mainRouter.route().handler(TimeoutHandler.create(120000))

        Configuration.setDefaultApiClient(createKubernetesClient(kubeConfig))

        mainRouter.post("/jobs/list").handler { context ->
            execute(context) { future ->
                future.complete(JobsListHandler.execute(portForward, useNodePort, fromJson(context, JobsListParams::class.java)))
            }
        }

        mainRouter.post("/job/run").handler { context ->
            execute(context) { future ->
                future.complete(JobRunHandler.execute("flink-controller", fromJson(context, JobRunParams::class.java)))
            }
        }

        mainRouter.post("/job/cancel").handler { context ->
            execute(context) { future ->
                future.complete(JobCancelHandler.execute(portForward, useNodePort, fromJson(context, JobCancelParams::class.java)))
            }
        }

        mainRouter.post("/job/scale").handler { context ->
            execute(context) { future ->
                future.complete(JobScaleHandler.execute(portForward, useNodePort, fromJson(context, JobScaleParams::class.java)))
            }
        }

        mainRouter.post("/job/details").handler { context ->
            execute(context) { future ->
                future.complete(JobDetailsHandler.execute(portForward, useNodePort, fromJson(context, JobDescriptor::class.java)))
            }
        }

        mainRouter.post("/job/metrics").handler { context ->
            execute(context) { future ->
                future.complete(JobMetricsHandler.execute(portForward, useNodePort, fromJson(context, JobDescriptor::class.java)))
            }
        }

        mainRouter.post("/jobmanager/metrics").handler { context ->
            execute(context) { future ->
                future.complete(JobManagerMetricsHandler.execute(portForward, useNodePort, fromJson(context, Descriptor::class.java)))
            }
        }

        mainRouter.post("/taskmanagers/list").handler { context ->
            execute(context) { future ->
                future.complete(TaskManagersListHandler.execute(portForward, useNodePort, fromJson(context, Descriptor::class.java)))
            }
        }

        mainRouter.post("/taskmanager/details").handler { context ->
            execute(context) { future ->
                future.complete(TaskManagerDetailsHandler.execute(portForward, useNodePort, fromJson(context, TaskManagerDescriptor::class.java)))
            }
        }

        mainRouter.post("/taskmanager/metrics").handler { context ->
            execute(context) { future ->
                future.complete(TaskManagerMetricsHandler.execute(portForward, useNodePort, fromJson(context, TaskManagerDescriptor::class.java)))
            }
        }

        mainRouter.post("/cluster/create").handler { context ->
            execute(context) { future ->
                future.complete(ClusterCreateHandler.execute("flink-controller", fromJson(context, Cluster::class.java)))
            }
        }

        mainRouter.post("/cluster/delete").handler { context ->
            execute(context) { future ->
                future.complete(ClusterDeleteHandler.execute("flink-operator", fromJson(context, Descriptor::class.java)))
            }
        }

        mainRouter.options("/").handler { context ->
            context.response().setStatusCode(204).end()
        }

        return vertx.createHttpServer().requestHandler(mainRouter).rxListen(port)
    }

    private fun <T> fromJson(context: RoutingContext, clazz: Class<out T>) = Gson().fromJson(context.bodyAsString, clazz)

    private fun execute(context: RoutingContext, handler: (Future<String>) -> Unit) {
        vertx.rxExecuteBlocking<String>(handler).subscribe({ output ->
            context.response().setStatusCode(200).putHeader("content-type", "application/json").end(output)
        }, { error ->
            context.response().setStatusCode(500).end(makeError(error))
        })
    }
}
