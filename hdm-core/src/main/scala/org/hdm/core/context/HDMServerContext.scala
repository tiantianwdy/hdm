package org.hdm.core.context

import java.util.UUID

import com.typesafe.config.{Config, ConfigFactory}
import org.hdm.core.io.CompressionCodec
import org.hdm.core.serializer.{JavaSerializer, SerializerInstance}
import org.hdm.core.utils.Logging

import scala.concurrent.ExecutionContext
import scala.util.Try

/**
  * Created by tiantian on 23/06/17.
  */
class HDMServerContext(val defaultConf: Config) extends HDMContext with Serializable with Logging {

  val HDM_HOME = Try {
    defaultConf.getString("hdm.home")
  } getOrElse {
    "/home/tiantian/Dev/lib/hdm/hdm-core"
  }

  lazy val PLANER_PARALLEL_CPU_FACTOR = Try {
    defaultConf.getInt("hdm.planner.parallelism.cpu.factor")
  } getOrElse (HDMContext.CORES)

  lazy val PLANER_PARALLEL_NETWORK_FACTOR = Try {
    defaultConf.getInt("hdm.planner.parallelism.network.factor")
  } getOrElse (HDMContext.CORES)

  val PLANER_is_GROUP_INPUT = Try {
    defaultConf.getBoolean("hdm.planner.input.group")
  } getOrElse (false)

  val PLANER_INPUT_GROUPING_POLICY = Try {
    defaultConf.getString("hdm.planner.input.group-policy")
  } getOrElse ("weighted")

  val BLOCK_COMPRESS_IN_TRANSPORTATION = Try {
    defaultConf.getBoolean("hdm.io.network.block.compress")
  } getOrElse (true)

  val NETTY_BLOCK_SERVER_THREADS = Try {
    defaultConf.getInt("hdm.io.netty.server.threads")
  } getOrElse (HDMContext.CORES)

  val NETTY_BLOCK_CLIENT_THREADS = Try {
    defaultConf.getInt("hdm.io.netty.client.threads")
  } getOrElse (HDMContext.CORES)

  val NETTY_CLIENT_CONNECTIONS_PER_PEER = Try {
    defaultConf.getInt("hdm.io.netty.client.connection-per-peer")
  } getOrElse (HDMContext.CORES)

  val SCHEDULING_FACTOR_CPU = Try {
    defaultConf.getInt("hdm.scheduling.policy.factor.cpu")
  } getOrElse (1)

  val SCHEDULING_FACTOR_IO = Try {
    defaultConf.getInt("hdm.scheduling.policy.factor.io")
  } getOrElse (10)

  val SCHEDULING_FACTOR_NETWORK = Try {
    defaultConf.getInt("hdm.scheduling.policy.factor.network")
  } getOrElse (20)

  val BLOCK_SERVER_INIT = Try {
    defaultConf.getBoolean("hdm.io.netty.server.init")
  } getOrElse (true)

  val BLOCK_SERVER_PROTOCOL = Try {
    defaultConf.getString("hdm.io.network.protocol")
  } getOrElse ("netty")

  var NETTY_BLOCK_SERVER_PORT:Int = Try {
    defaultConf.getInt("hdm.io.netty.server.port")
  } getOrElse (9091)

  val SCHEDULING_POLICY_CLASS = Try {
    defaultConf.getString("hdm.scheduling.policy.class")
  } getOrElse ("org.hdm.core.scheduling.MinMinSchedulingOpt")

  lazy val DEFAULT_DEPENDENCY_BASE_PATH = Try {
    defaultConf.getString("hdm.dep.base.path")
  } getOrElse ("target/repo/hdm")

  lazy val parallelismFactor = Try {
    defaultConf.getDouble("hdm.executor.parallelism.factor")
  } getOrElse (1.0D)

  lazy val mockExecution = Try {
    defaultConf.getBoolean("hdm.executor.mockExecution")
  } getOrElse (false)

  val MAX_MEM_GC_SIZE = Try {
    defaultConf.getInt("hdm.memory.gc.max.byte")
  } getOrElse (1024 * 1024 * 1024) // about 256MB


  val JOB_DEFAULT_WAITING_TIMEOUT = Try {
    defaultConf.getLong("hdm.executor.job.timeout.default")
  } getOrElse (600000L) // 10 mins

  lazy val DEFAULT_BLOCK_ID_LENGTH = defaultSerializer.serialize(newLocalId()).array().length

  val defaultSerializer: SerializerInstance = new JavaSerializer(defaultConf).newInstance()

  val defaultSerializerFactory = new JavaSerializer(defaultConf)

//  val jobSerializerFactory = new KryoSerializer(defaultConf)

//  val jobSerializer: SerializerInstance = new KryoSerializer(defaultConf).newInstance()

  val cores = HDMContext.CORES

  val executionContext = ExecutionContext.global

  def getCompressor(): CompressionCodec = {
    if (BLOCK_COMPRESS_IN_TRANSPORTATION) compressor
    else null
  }


  def newClusterId(): String = {
    UUID.randomUUID().toString
  }

  def newLocalId(): String = {
    UUID.randomUUID().toString
  }



}

object HDMServerContext {


  lazy val defaultContext = apply()


  def apply() = {
    if (HDMContext.defaultConf == null) {
      HDMContext.setDefaultConf(ConfigFactory.load("hdm-core.conf"))
    }
    new HDMServerContext(HDMContext.defaultConf )
  }

  def apply(conf: Config) = {
    HDMContext.setDefaultConf(conf)
    new HDMServerContext(conf)
  }
}