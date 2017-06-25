package org.hdm.core.context

import com.typesafe.config.{ConfigFactory, Config}
import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor.ClusterExecutorContext
import org.hdm.core.message.{SerializedJobMsg, RegisterPromiseMsg}
import org.hdm.core.model.HDM
import org.hdm.core.serializer.{JavaSerializer, SerializerInstance}
import org.hdm.core.storage.HDMBlockManager
import org.hdm.core.utils.Logging

import scala.concurrent.{Promise, Future, ExecutionContext}
import scala.util.Try

/**
  * Created by tiantian on 22/06/17.
  */
class HDMAppContext(defaultConf: Config) extends HDMContext with Serializable with Logging {

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


  override val executionContext: ExecutionContext = ClusterExecutorContext()


  override val defaultSerializer: SerializerInstance = new JavaSerializer(defaultConf).newInstance()


  override def init(leader: String, slots: Int = 0): Unit = {
    startAsClient(masterPath = leader, localExecution = false)
  }

  def startAsClient(masterPath: String, port: Int = 20010, blockPort: Int = 9092, conf: Config = defaultConf, localExecution: Boolean = false) {
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerFollower", masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
    leaderPath.set(masterPath)
    this.NETTY_BLOCK_SERVER_PORT = blockPort
    if (BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(this)
//    if (localExecution) {
//      this.slot.set(CORES)
//      SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
//        "org.hdm.core.coordinator.HDMClusterWorkerActor",
//        HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, CORES, blockContext))
//    }
    addShutdownHook(AppContext.defaultAppContext)
  }


  def submitJob(master: String, appName: String, version: String, hdm: HDM[_], parallel: Int): Future[HDM[_]] = synchronized {
    val rootPath = SmsSystem.physicalRootPath
    //    HDMContext.declareHdm(Seq(hdm))
    val promise = SmsSystem.askLocalMsg(HDMContext.JOB_RESULT_DISPATCHER, RegisterPromiseMsg(appName, version, hdm.id, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[HDM[_]]]
      case none => null
    }
    val masterURL = if (clusterExecution.get()) master + "/" + HDMContext.CLUSTER_RESOURCE_MANAGER_NAME
    else  master + "/" + HDMContext.CLUSTER_EXECUTOR_NAME
    val start = System.currentTimeMillis()
    val jobBytes = HDMContext.JOB_SERIALIZER.serialize(hdm).array
    val end = System.currentTimeMillis()
    log.info(s"Completed serializing task with size: ${jobBytes.length / 1024} KB. in ${end - start} ms.")
    val jobMsg = new SerializedJobMsg(appName, version, jobBytes, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER, rootPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, parallel)
    SmsSystem.askAsync(masterURL, jobMsg)
    log.info(s"Sending a job [${hdm.id}] to ${masterURL} for execution.")
    if (promise ne null) promise.future
    else throw new Exception("add job dispatcher failed.")
  }

  def compute(hdm: HDM[_], parallelism: Int): Future[HDM[_]] = {
    //    addJob(hdm.id, explain(hdm, parallelism))
    submitJob(hdm.appContext.masterPath,
      hdm.appContext.appName,
      hdm.appContext.version,
      hdm, parallelism)
  }


  def addShutdownHook(appContext: AppContext): Unit ={
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        log.info(s"HDMContext is shuting down...")
        shutdown(appContext)
        Thread.sleep(1000)
        SmsSystem.shutDown()
        log.info(s"HDMContext has shut down successfully..")
      }
    })
  }
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