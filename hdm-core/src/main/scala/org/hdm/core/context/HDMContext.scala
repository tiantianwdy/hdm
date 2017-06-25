package org.hdm.core.context

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}

import com.typesafe.config.{Config, ConfigFactory}
import org.hdm.akka.server.SmsSystem
import org.hdm.core.coordinator.{ClusterResourceWorkerSpec, HDMWorkerParams}
import org.hdm.core.executor.{Task, ClusterExecutorContext}
import org.hdm.core.functions.SerializableFunction
import org.hdm.core.io.netty.NettyConnectionManager
import org.hdm.core.io.{CompressionCodec, SnappyCompressionCodec}
import org.hdm.core.message._
import org.hdm.core.model.{GroupedSeqHDM, HDM, KvHDM, ParHDM}
import org.hdm.core.planing.{StaticMultiClusterPlanner, StaticPlaner}
import org.hdm.core.scheduling.{AdvancedScheduler, MultiClusterScheduler, SchedulingPolicy}
import org.hdm.core.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.hdm.core.server._
import org.hdm.core.storage.{Block, HDMBlockManager}
import org.hdm.core.utils.Logging

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.Try

/**
 * Created by Tiantian on 2014/11/4.
 */
trait HDMContext {

  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  val leaderPath: AtomicReference[String] = new AtomicReference[String]()

  val slot = new AtomicInteger(1)

  val CORES = Runtime.getRuntime.availableProcessors

  val BLOCK_COMPRESS_IN_TRANSPORTATION: Boolean

  val BLOCK_SERVER_PROTOCOL: String

  var NETTY_BLOCK_SERVER_PORT:Int

  val NETTY_BLOCK_SERVER_THREADS: Int

  val NETTY_CLIENT_CONNECTIONS_PER_PEER: Int

  val NETTY_BLOCK_CLIENT_THREADS : Int

  val JOB_DEFAULT_WAITING_TIMEOUT: Long

  val MAX_MEM_GC_SIZE: Int

  val PLANER_PARALLEL_NETWORK_FACTOR: Int

  val executionContext: ExecutionContext

  val defaultSerializer: SerializerInstance

  val clusterExecution = new AtomicBoolean(true)

  val compressor = HDMContext.DEFAULT_COMPRESSOR

  def init(leader: String, slots: Int): Unit

  def compute(hdm: HDM[_], parallelism: Int): Future[HDM[_]]

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

  def clean(appId: String) = {
    //todo clean all the resources used by this application
    val masterURL = leaderPath.get() + "/" + HDMContext.CLUSTER_RESOURCE_MANAGER_NAME
    SmsSystem.askAsync(masterURL, ApplicationShutdown(appId))
  }

  def shutdown(appContext: AppContext = AppContext.defaultAppContext) {
    clean(appContext.appName + "#" + appContext.version)
    SmsSystem.shutDown()
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

  def addTask(task: Task[_, _]) = {
    SmsSystem.askAsync(leaderPath.get() + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, AddTaskMsg(task))
  }
}




object HDMContext extends Logging {

  val _defaultConf = new AtomicReference[Config]()

  private val jobSerializer = new ThreadLocal[SerializerInstance]()

  val HDM_VERSION = Try {
    defaultConf.getString("hdm.version")
  } getOrElse ("0.1-SNAPSHOT")

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

  def defaultConf() = _defaultConf.get()

  def setDefaultConf(conf: Config) = _defaultConf.set(conf)

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