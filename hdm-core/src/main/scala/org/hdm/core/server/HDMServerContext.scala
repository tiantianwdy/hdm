package org.hdm.core.server

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import com.typesafe.config.{ConfigFactory, Config}
import org.hdm.akka.server.SmsSystem
import org.hdm.core.context.{HDMContext, AppContext, BlockContext}
import org.hdm.core.coordinator.{ClusterResourceWorkerSpec, HDMWorkerParams}
import org.hdm.core.executor.{ClusterExecutorContext, Task}
import org.hdm.core.io.CompressionCodec
import org.hdm.core.message._
import org.hdm.core.model.{HDM, ParHDM}
import org.hdm.core.planing.{StaticMultiClusterPlanner, StaticPlaner}
import org.hdm.core.scheduling.{AdvancedScheduler, MultiClusterScheduler, SchedulingPolicy}
import org.hdm.core.serializer.{JavaSerializer, SerializerInstance}
import org.hdm.core.storage.{Block, HDMBlockManager}
import org.hdm.core.utils.Logging

import scala.concurrent.{Future, Promise}
import scala.util.Try

/**
  * Created by tiantian on 23/06/17.
  */
class HDMServerContext(defaultConf: Config) extends HDMContext with Serializable with Logging {

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

  implicit lazy val executionContext = ClusterExecutorContext((cores * parallelismFactor).toInt)


  val planer = new StaticPlaner(this)


  private var hdmBackEnd: ServerBackend = null

  lazy val schedulingPolicy = Class.forName(SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]

  //  val scheduler = new SimpleFIFOScheduler


//  def blockContext() = {
//    BlockContext(leaderPath.get() + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
//  }


  def startAsClusterMaster(host:String = "", port: Int = 8999, conf: Config = defaultConf,  mode: String = "stand-alone"): Unit = {
    SmsSystem.startAsMaster(host, port, isLinux, conf)
    val masterCls = if (mode == "stand-alone") "org.hdm.core.coordinator.ClusterResourceLeader"
    else "org.hdm.core.coordinator.ClusterResourceLeader"
    SmsSystem.addActor(HDMContext.CLUSTER_RESOURCE_MANAGER_NAME, "localhost", masterCls, null)
//    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerLeader", null)
//    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    leaderPath.set(SmsSystem.physicalRootPath)
    addShutdownHook(AppContext.defaultAppContext)
  }


  def startAsClusterSlave(masterPath: String, host:String = "", port: Int = 12010, blockPort: Int = 9091, conf: Config = defaultConf, slots: Int = cores, mem:String): Unit = {
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(HDMContext.CLUSTER_RESOURCE_WORKER_NAME, "localhost",
      "org.hdm.core.coordinator.ClusterResourceWorker",
      ClusterResourceWorkerSpec(masterPath + "/" + HDMContext.CLUSTER_RESOURCE_MANAGER_NAME, slots, mem, HDM_HOME))
    leaderPath.set(masterPath)
    addShutdownHook(AppContext.defaultAppContext)
  }


  def startAsMaster(host:String = "", port: Int = 8999, conf: Config = defaultConf, slots: Int = 0, mode: String = "single-cluster") {
    this.slot.set(slots)
    SmsSystem.startAsMaster(host, port, isLinux, conf)
    //    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.ClusterExecutorLeader", slots)
    //    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.HDMClusterLeaderActor", slots)
    val masterCls = if (mode == "multi-cluster") "org.hdm.core.coordinator.HDMMultiClusterLeader"
    else "org.hdm.core.coordinator.SingleCoordinationLeader"
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost", masterCls, slots)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerLeader", null)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    leaderPath.set(SmsSystem.physicalRootPath)
    addShutdownHook(AppContext.defaultAppContext)
  }

  def startAsSlave(masterPath: String, host:String = "", port: Int = 10010, blockPort: Int = 9091, conf: Config = defaultConf, slots: Int = cores) {
    this.slot.set(slots)
    this.NETTY_BLOCK_SERVER_PORT = blockPort
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
      "org.hdm.core.coordinator.HDMClusterWorkerActor",
      HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, slots, blockContext))
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost",
      "org.hdm.core.coordinator.BlockManagerFollower",
      masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost",
      "org.hdm.core.coordinator.ResultHandler", null)
    leaderPath.set(masterPath)
    if (BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(this)
    addShutdownHook(AppContext.defaultAppContext)
  }

  def startAsClient(masterPath: String, port: Int = 20010, blockPort: Int = 9092, conf: Config = defaultConf, localExecution: Boolean = false) {
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerFollower", masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, BLOCK_SERVER_PROTOCOL, NETTY_BLOCK_SERVER_PORT)
    leaderPath.set(masterPath)
    this.NETTY_BLOCK_SERVER_PORT = blockPort
    if (BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(this)
    if (localExecution) {
      this.slot.set(cores)
      SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
        "org.hdm.core.coordinator.HDMClusterWorkerActor",
        HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, cores, blockContext))
    }
    addShutdownHook(AppContext.defaultAppContext)
  }

  def init(leader: String = "localhost", slots: Int = cores) {

    if (leader == "localhost") {
      startAsMaster(slots = slots)
    } else {
      if (slots > 0)
        startAsClient(masterPath = leader, localExecution = true)
      else
        startAsClient(masterPath = leader, localExecution = false)
    }
    //    scheduler.start()
  }


  def getServerBackend(mode: String = "single"): ServerBackend = {
    if (hdmBackEnd == null) mode match {
      case "single" =>
        val appManager = new AppManager
        val blockManager = HDMBlockManager()
        val promiseManager = new DefPromiseManager
        val resourceManager = new SingleClusterResourceManager
        val schedulingPolicy = Class.forName(SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]
        //    val scheduler = new DefScheduler(blockManager, promiseManager, resourceManager, SmsSystem.system)
        val scheduler = new AdvancedScheduler(blockManager, promiseManager, resourceManager, ProvenanceManager(), SmsSystem.system, schedulingPolicy)
        hdmBackEnd = new HDMServerBackend(blockManager, scheduler, planer, resourceManager, promiseManager, DependencyManager(), this)
        log.info(s"created new HDMServerBackend with scheduling: ${SCHEDULING_POLICY_CLASS}")

      case "multiple" =>
        val appManager = new AppManager
        val blockManager = HDMBlockManager()
        val promiseManager = new DefPromiseManager
        val resourceManager = new MultiClusterResourceManager
        val schedulingPolicy = Class.forName(SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]
        val multiPlanner = new StaticMultiClusterPlanner(planer, HDMServerContext.defaultContext)
        val scheduler = new MultiClusterScheduler(blockManager, promiseManager, resourceManager, ProvenanceManager(), SmsSystem.system, DependencyManager(), multiPlanner, schedulingPolicy, this)
        hdmBackEnd = new MultiClusterBackend(blockManager, scheduler, multiPlanner, resourceManager, promiseManager, DependencyManager(), this)
        log.info(s"created new MultiClusterBackend with scheduling: ${SCHEDULING_POLICY_CLASS}")
    }
    hdmBackEnd
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


  def explain(hdm: HDM[_], parallelism: Int) = {
    //    val hdmOpt = new FunctionFusion().optimize(hdm)
    //    planer.plan(hdmOpt, parallelism)
    planer.plan(hdm, parallelism)
  }

  def compute(hdm: HDM[_], parallelism: Int): Future[HDM[_]] = {
    //    addJob(hdm.id, explain(hdm, parallelism))
    submitJob(hdm.appContext.masterPath,
      hdm.appContext.appName,
      hdm.appContext.version,
      hdm, parallelism)
  }

  @Deprecated
  def submitTasks(appId: String, hdms: Seq[ParHDM[_, _]]): Future[ParHDM[_, _]] = {
    val rootPath = SmsSystem.rootPath
    this.declareHdm(hdms)
    val promise = SmsSystem.askLocalMsg(HDMContext.JOB_RESULT_DISPATCHER, AddHDMsMsg(appId, hdms, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[ParHDM[_, _]]]
      case none => null
    }
    SmsSystem.askAsync(leaderPath.get() + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, AddHDMsMsg(appId, hdms, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER))

    if (promise ne null) promise.future
    else throw new Exception("add job dispatcher failed.")
  }



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