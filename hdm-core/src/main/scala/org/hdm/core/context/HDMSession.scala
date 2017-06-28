package org.hdm.core.context

import com.typesafe.config.Config
import org.hdm.akka.server.SmsSystem
import org.hdm.core.message.{SerializedJobMsg, RegisterPromiseMsg}
import org.hdm.core.model.HDM
import org.hdm.core.storage.HDMBlockManager
import org.hdm.core.utils.Logging

import scala.concurrent.{ExecutionContext, Promise, Future}

/**
  * Created by tiantian on 26/06/17.
  */
class HDMSession(val hDMContext:HDMContext) extends HDMEntry with Logging {

  override val executionContext: ExecutionContext = ExecutionContext.global

  override def init(leader: String, slots: Int = 0, port: Int = 20010): Unit = {
    startAsClient(masterPath = leader, port = port)
  }

  def startAsClient(masterPath: String, port: Int = 20010, blockPort: Int = 9092, conf: Config = hDMContext.defaultConf) {
    SmsSystem.startAsSlave(masterPath, port, hDMContext.isLinux, conf)
    SmsSystem.addActor(HDMContext.BLOCK_MANAGER_NAME, "localhost", "org.hdm.core.coordinator.BlockManagerFollower", masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME)
    SmsSystem.addActor(HDMContext.JOB_RESULT_DISPATCHER, "localhost", "org.hdm.core.coordinator.ResultHandler", null)
    val blockContext = BlockContext(masterPath + "/" + HDMContext.BLOCK_MANAGER_NAME, hDMContext.BLOCK_SERVER_PROTOCOL, hDMContext.NETTY_BLOCK_SERVER_PORT)
    hDMContext.leaderPath.set(masterPath)
    hDMContext.NETTY_BLOCK_SERVER_PORT = blockPort
    if (hDMContext.BLOCK_SERVER_INIT) HDMBlockManager.initBlockServer(hDMContext)
    addShutdownHook(AppContext.defaultAppContext)
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

  def compute(hdm: HDM[_], parallelism: Int): Future[HDM[_]] = {
    //    addJob(hdm.id, explain(hdm, parallelism))
    submitJob(hdm.appContext.masterPath,
      hdm.appContext.appName,
      hdm.appContext.version,
      hdm, parallelism)
  }


  override def addShutdownHook(appContext: AppContext): Unit ={
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
