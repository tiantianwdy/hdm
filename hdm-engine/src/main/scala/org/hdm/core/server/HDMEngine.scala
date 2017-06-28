package org.hdm.core.server

import java.util.concurrent.atomic.AtomicReference

import com.typesafe.config.Config
import org.hdm.akka.server.SmsSystem
import org.hdm.core.context._
import org.hdm.core.coordinator.{HDMWorkerParams, ClusterResourceWorkerSpec}
import org.hdm.core.executor.ClusterExecutorContext
import org.hdm.core.message.{AddHDMsMsg, SerializedJobMsg, RegisterPromiseMsg}
import org.hdm.core.model.{ParHDM, HDM}
import org.hdm.core.planing.{StaticMultiClusterPlanner, StaticPlaner}
import org.hdm.core.scheduling.{MultiClusterScheduler, AdvancedScheduler, SchedulingPolicy}
import org.hdm.core.server._
import org.hdm.core.storage.HDMBlockManager
import org.hdm.core.utils.Logging

import scala.concurrent.{Promise, Future}

/**
  * Created by tiantian on 26/06/17.
  */
class HDMEngine(val hDMContext:HDMServerContext) extends HDMEntry with Logging {

  implicit lazy val executionContext = ClusterExecutorContext((hDMContext.cores * hDMContext.parallelismFactor).toInt)

  val planer = new StaticPlaner(hDMContext)

  private var hdmBackEnd: ServerBackend = null

  lazy val schedulingPolicy = Class.forName(hDMContext.SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]


  def startAsClusterMaster(host:String = "", port: Int = 8999, conf: Config = hDMContext.defaultConf,  mode: String = "stand-alone"): Unit = {
    SmsSystem.startAsMaster(host, port, hDMContext.isLinux, conf)
    val masterCls = if (mode == "stand-alone") "org.hdm.core.coordinator.ClusterResourceLeader"
    else "org.hdm.core.coordinator.ClusterResourceLeader"
    SmsSystem.addActor(HDMContext.CLUSTER_RESOURCE_MANAGER_NAME, "localhost", masterCls, null)
    //    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerLeader", null)
    //    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    hDMContext.leaderPath.set(SmsSystem.physicalRootPath)
    addShutdownHook(AppContext.defaultAppContext)
  }


  def startAsClusterSlave(masterPath: String, host:String = "", port: Int = 12010, blockPort: Int = 9091, conf: Config = hDMContext.defaultConf, slots: Int = hDMContext.cores, mem:String): Unit = {
    SmsSystem.startAsSlave(masterPath, port, hDMContext.isLinux, conf)
    SmsSystem.addActor(HDMContext.CLUSTER_RESOURCE_WORKER_NAME, "localhost",
      "org.hdm.core.coordinator.ClusterResourceWorker",
      ClusterResourceWorkerSpec(masterPath + "/" + HDMContext.CLUSTER_RESOURCE_MANAGER_NAME, slots, mem, hDMContext.HDM_HOME))
    hDMContext.leaderPath.set(masterPath)
    addShutdownHook(AppContext.defaultAppContext)
  }


  def startAsMaster(host:String = "", port: Int = 8999, conf: Config = hDMContext.defaultConf, slots: Int = 0, mode: String = "single-cluster") {
    hDMContext.slot.set(slots)
    SmsSystem.startAsMaster(host, port, hDMContext.isLinux, conf)
    //    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.ClusterExecutorLeader", slots)
    //    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost","org.hdm.core.coordinator.HDMClusterLeaderActor", slots)
    val masterCls = if (mode == "multi-cluster") "org.hdm.core.coordinator.HDMMultiClusterLeader"
    else "org.hdm.core.coordinator.SingleCoordinationLeader"
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost", masterCls, slots)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerLeader", null)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    hDMContext.leaderPath.set(SmsSystem.physicalRootPath)
    addShutdownHook(AppContext.defaultAppContext)
  }

  def startAsSlave(masterPath: String, host:String = "", port: Int = 10010, blockPort: Int = 9091, conf: Config = hDMContext.defaultConf, slots: Int = hDMContext.cores) {
    hDMContext.slot.set(slots)
    hDMContext.NETTY_BLOCK_SERVER_PORT = blockPort
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, hDMContext.BLOCK_SERVER_PROTOCOL, hDMContext.NETTY_BLOCK_SERVER_PORT)
    SmsSystem.startAsSlave(masterPath, port, hDMContext.isLinux, conf)
    SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
      "org.hdm.core.coordinator.HDMClusterWorkerActor",
      HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, slots, blockContext))
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost",
      "org.hdm.core.coordinator.BlockManagerFollower",
      masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost",
      "org.hdm.core.coordinator.ResultHandler", null)
    hDMContext.leaderPath.set(masterPath)
    if (hDMContext.BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(hDMContext)
    addShutdownHook(AppContext.defaultAppContext)
  }

  def startAsClient(masterPath: String, port: Int = 20010, blockPort: Int = 9092, conf: Config = hDMContext.defaultConf, localExecution: Boolean = false) {
    SmsSystem.startAsSlave(masterPath, port, hDMContext.isLinux, conf)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerFollower", masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, hDMContext.BLOCK_SERVER_PROTOCOL, hDMContext.NETTY_BLOCK_SERVER_PORT)
    hDMContext.leaderPath.set(masterPath)
    hDMContext.NETTY_BLOCK_SERVER_PORT = blockPort
    if (hDMContext.BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(hDMContext)
    if (localExecution) {
      hDMContext.slot.set(hDMContext.cores)
      SmsSystem.addActor(HDMContext.CLUSTER_EXECUTOR_NAME, "localhost",
        "org.hdm.core.coordinator.HDMClusterWorkerActor",
        HDMWorkerParams(masterPath + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, hDMContext.cores, blockContext))
    }
    addShutdownHook(AppContext.defaultAppContext)
  }

  def init(leader: String = "localhost", slots: Int = hDMContext.cores, port:Int = 8999) {

    if (leader == "localhost") {
      startAsMaster(slots = slots, port = port)
    } else {
      if (slots > 0)
        startAsClient(masterPath = leader, localExecution = true, port = port)
      else
        startAsClient(masterPath = leader, localExecution = false, port = port)
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
        val schedulingPolicy = Class.forName(hDMContext.SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]
        //    val scheduler = new DefScheduler(blockManager, promiseManager, resourceManager, SmsSystem.system)
        val scheduler = new AdvancedScheduler(blockManager, promiseManager, resourceManager, ProvenanceManager(), SmsSystem.system, schedulingPolicy)
        hdmBackEnd = new HDMServerBackend(blockManager, scheduler, planer, resourceManager, promiseManager, DependencyManager(), hDMContext)
        log.info(s"created new HDMServerBackend with scheduling: ${hDMContext.SCHEDULING_POLICY_CLASS}")

      case "multiple" =>
        val appManager = new AppManager
        val blockManager = HDMBlockManager()
        val promiseManager = new DefPromiseManager
        val resourceManager = new MultiClusterResourceManager
        val schedulingPolicy = Class.forName(hDMContext.SCHEDULING_POLICY_CLASS).newInstance().asInstanceOf[SchedulingPolicy]
        val multiPlanner = new StaticMultiClusterPlanner(planer, HDMServerContext.defaultContext)
        val scheduler = new MultiClusterScheduler(blockManager, promiseManager, resourceManager, ProvenanceManager(), SmsSystem.system, DependencyManager(), multiPlanner, schedulingPolicy, this)
        hdmBackEnd = new MultiClusterBackend(blockManager, scheduler, multiPlanner, resourceManager, promiseManager, DependencyManager(), hDMContext)
        log.info(s"created new MultiClusterBackend with scheduling: ${hDMContext.SCHEDULING_POLICY_CLASS}")
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
    val masterURL = if (hDMContext.clusterExecution.get()) master + "/" + HDMContext.CLUSTER_RESOURCE_MANAGER_NAME
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
    declareHdm(hdms)
    val promise = SmsSystem.askLocalMsg(HDMContext.JOB_RESULT_DISPATCHER, AddHDMsMsg(appId, hdms, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[ParHDM[_, _]]]
      case none => null
    }
    SmsSystem.askAsync(hDMContext.leaderPath.get() + "/" + HDMContext.CLUSTER_EXECUTOR_NAME, AddHDMsMsg(appId, hdms, rootPath + "/" + HDMContext.JOB_RESULT_DISPATCHER))

    if (promise ne null) promise.future
    else throw new Exception("add job dispatcher failed.")
  }

  override def addShutdownHook(appContext: AppContext): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        log.info(s"HDMEngine is shutting down...")
        shutdown(appContext)
        Thread.sleep(1000)
        SmsSystem.shutDown()
        log.info(s"HDMEngine has shut down successfully..")
      }
    })
  }

}


object HDMEngine {

  val _engine = new AtomicReference[HDMEngine]

  def apply(): HDMEngine ={
    if(_engine.get() eq null) _engine.set(new HDMEngine(HDMServerContext.defaultContext))
    _engine.get()
  }
}