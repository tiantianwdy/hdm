package org.hdm.core.context

import com.typesafe.config.{Config, ConfigFactory}
import org.hdm.core.serializer.{JavaSerializer, SerializerInstance}
import org.hdm.core.utils.Logging

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Created by tiantian on 22/06/17.
  */
class HDMAppContext(val defaultConf: Config) extends HDMContext with Serializable with Logging {

  val BLOCK_COMPRESS_IN_TRANSPORTATION = Try {
    defaultConf.getBoolean("hdm.io.network.block.compress")
  } getOrElse (true)

  val NETTY_BLOCK_SERVER_THREADS = Try {
    defaultConf.getInt("hdm.io.netty.server.threads")
  } getOrElse (CORES)

  val NETTY_BLOCK_CLIENT_THREADS = Try {
    defaultConf.getInt("hdm.io.netty.client.threads")
  } getOrElse (CORES)

  val NETTY_CLIENT_CONNECTIONS_PER_PEER = Try {
    defaultConf.getInt("hdm.io.netty.client.connection-per-peer")
  } getOrElse (CORES)

  val BLOCK_SERVER_INIT = Try {
    defaultConf.getBoolean("hdm.io.netty.server.init")
  } getOrElse (true)

  val BLOCK_SERVER_PROTOCOL = Try {
    defaultConf.getString("hdm.io.network.protocol")
  } getOrElse ("netty")

  var NETTY_BLOCK_SERVER_PORT:Int = Try {
    defaultConf.getInt("hdm.io.netty.server.port")
  } getOrElse (9091)

  val JOB_DEFAULT_WAITING_TIMEOUT = Try {
    defaultConf.getLong("hdm.executor.job.timeout.default")
  } getOrElse (600000L)

  lazy val PLANER_PARALLEL_CPU_FACTOR = Try {
    defaultConf.getInt("hdm.planner.parallelism.cpu.factor")
  } getOrElse (HDMContext.CORES)

  lazy val PLANER_PARALLEL_NETWORK_FACTOR = Try {
    defaultConf.getInt("hdm.planner.parallelism.network.factor")
  } getOrElse (HDMContext.CORES)

  val MAX_MEM_GC_SIZE: Int = Try {
    defaultConf.getInt("hdm.memory.gc.max.byte")
  } getOrElse (1024 * 1024 * 1024) // about 256MB

  val defaultSerializer: SerializerInstance = new JavaSerializer(defaultConf).newInstance()

  override def executionContext: ExecutionContext = ExecutionContext.global
}


object HDMAppContext{

  lazy val defaultContext = apply()


  def apply() = {
    if (HDMContext.defaultConf == null) {
      HDMContext.setDefaultConf(ConfigFactory.load("hdm-core.conf"))
    }
    new HDMAppContext(HDMContext.defaultConf )
  }

  def apply(conf: Config) = {
    HDMContext.setDefaultConf(conf)
    new HDMAppContext(conf)
  }
}