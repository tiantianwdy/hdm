package org.hdm.core.context

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import com.typesafe.config.Config
import org.hdm.akka.server.SmsSystem
import org.hdm.core.io.SnappyCompressionCodec
import org.hdm.core.io.netty.NettyConnectionManager
import org.hdm.core.message._
import org.hdm.core.model.{GroupedSeqHDM, HDM, KvHDM, ParHDM}
import org.hdm.core.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.hdm.core.storage.Block
import org.hdm.core.utils.Logging

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by Tiantian on 2014/11/4.
 */
trait HDMContext {

  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  val leaderPath: AtomicReference[String] = new AtomicReference[String]()

  val slot = new AtomicInteger(1)

  val defaultConf:Config

  val CORES = Runtime.getRuntime.availableProcessors

  val BLOCK_COMPRESS_IN_TRANSPORTATION: Boolean

  val BLOCK_SERVER_PROTOCOL: String

  val BLOCK_SERVER_INIT: Boolean

  var NETTY_BLOCK_SERVER_PORT:Int

  val NETTY_BLOCK_SERVER_THREADS: Int

  val NETTY_CLIENT_CONNECTIONS_PER_PEER: Int

  val NETTY_BLOCK_CLIENT_THREADS : Int

  val JOB_DEFAULT_WAITING_TIMEOUT: Long

  val MAX_MEM_GC_SIZE: Int

  val PLANER_PARALLEL_CPU_FACTOR: Int

  val PLANER_PARALLEL_NETWORK_FACTOR: Int

  val defaultSerializer: SerializerInstance

  val clusterExecution = new AtomicBoolean(true)

  def executionContext: ExecutionContext

  val compressor = HDMContext.DEFAULT_COMPRESSOR

  def clusterBlockPath = {
    leaderPath.get() + "/" + HDMContext.BLOCK_MANAGER_NAME
  }

  def blockContext() = {
    BlockContext(leaderPath.get() + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
  }

  def localBlockPath = {
    BLOCK_SERVER_PROTOCOL match {
      case "akka" => SmsSystem.physicalRootPath + "/" + HDMContext.BLOCK_MANAGER_NAME
      case "netty" => s"netty://${NettyConnectionManager.localHost}:${NETTY_BLOCK_SERVER_PORT}"
    }
  }


//  def addTask(task: Task[_, _]) = {
//    SmsSystem.askAsync(leaderPath.get() + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, AddTaskMsg(task))
//  }
}




object HDMContext extends Logging {

  val _defaultConf = new AtomicReference[Config]()

  private val jobSerializer = new ThreadLocal[SerializerInstance]()

  val defaultContext:HDMContext = HDMServerContext.defaultContext

  def defaultConf() = _defaultConf.get()

  def setDefaultConf(conf: Config) = _defaultConf.set(conf)

  val HDM_VERSION = Try {
    defaultConf.getString("hdm.version")
  } getOrElse ("0.1-SNAPSHOT")
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

  val CLUSTER_RESOURCE_MANAGER_NAME = "ClusterResourceLeader"

  val CLUSTER_RESOURCE_WORKER_NAME = "ClusterResourceWorker"

  val CLUSTER_EXECUTOR_NAME: String = "ClusterExecutor"

  val BLOCK_MANAGER_NAME: String = "BlockManager"

  val JOB_RESULT_DISPATCHER: String = "ResultDispatcher"

  val DEFAULT_SERIALIZER: SerializerInstance = new JavaSerializer(defaultConf).newInstance()

  val DEFAULT_SERIALIZER_FACTORY = new JavaSerializer(defaultConf)

  def JOB_SERIALIZER: SerializerInstance = {
    if(jobSerializer.get() == null){
      jobSerializer.set(new KryoSerializer(defaultConf).newInstance())
    }
    jobSerializer.get()
  }

  def reNewJobSerializer(ser:SerializerInstance): Unit = {
    jobSerializer.set(ser)
  }

  val DEFAULT_COMPRESSOR = new SnappyCompressionCodec(defaultConf)

  lazy val DEFAULT_BLOCK_ID_LENGTH = DEFAULT_SERIALIZER.serialize(newLocalId()).array().length

  val CORES = Runtime.getRuntime.availableProcessors

  def newClusterId(): String = {
    UUID.randomUUID().toString
  }

  def newLocalId(): String = {
    UUID.randomUUID().toString
  }


  def declareHdm(hdms: Seq[ParHDM[_, _]], declare: Boolean = true) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddRefMsg(hdms, declare))
  }

  def addBlock(block: Block[_], declare: Boolean) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, AddBlockMsg(block, declare))
  }

  def queryBlock(id: String, location: String) = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, QueryBlockMsg(Seq(id), location))
  }

  def removeBlock(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveBlockMsg(id))
  }

  def removeRef(id: String): Unit = {
    SmsSystem.forwardLocalMsg(HDMContext.BLOCK_MANAGER_NAME, RemoveRefMsg(id))
  }

  /**
   *
   * @param hdm
   * @tparam K
   * @tparam V
   * @return
   */
  implicit def hdmToKVHDM[K: ClassTag, V: ClassTag](hdm: HDM[(K, V)]): KvHDM[K, V] = {
    new KvHDM(hdm)
  }

  implicit def hdmToGroupedSeqHDM[K: ClassTag, V: ClassTag](hdm: ParHDM[_, (K, Iterable[V])]): GroupedSeqHDM[K, V] = {
    new GroupedSeqHDM[K, V](hdm)
  }
}