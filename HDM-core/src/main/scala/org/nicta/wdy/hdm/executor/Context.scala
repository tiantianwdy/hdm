package org.nicta.wdy.hdm.executor

import scala.concurrent.{Promise, Future}
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.functions.{ParallelFunction, DDMFunction_1, SerializableFunction}
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager}
import java.util.concurrent.atomic.AtomicReference
import org.nicta.wdy.hdm.server.HDMServer
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.message.{AddBlockMsg, AddJobMsg, AddTaskMsg, AddRefMsg}
import java.util.UUID

/**
 * Created by Tiantian on 2014/11/4.
 */
trait Context {

  def findHDM(id:String): HDM[_, _] = ???

  def sendFunc[T,R](target:HDM[_,T], func:SerializableFunction[T,R]): Future[HDM[T,R]] = ???

  def receiveFunc[T,R](target:HDM[_, T], func:SerializableFunction[T,R]): Future[HDM[T,R]] = ???

  def runTask[T,R](target:HDM[_, T], func:SerializableFunction[T,R]): Future[HDM[T,R]] = ???
}

object HDMContext extends  Context{

  implicit lazy val executionContext = ClusterExecutorContext(CORES)

  lazy val defaultConf = ConfigFactory.load()

  val CORES = Runtime.getRuntime.availableProcessors

  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  val blockManager = HDMBlockManager()

  val planer = LocalPlaner

  val scheduler = new SimpleFIFOScheduler

  val leaderPath: AtomicReference[String] = new AtomicReference[String]()

  val CLUSTER_EXECUTOR_NAME:String =  "ClusterExecutor"

  val BLOCK_MANAGER_NAME:String =  "BlockManager"

  val JOB_RESULT_DISPATCHER:String = "ResultDispatcher"

  def clusterContextPath = leaderPath.get()

  def localContextPath = SmsSystem.rootPath

  def startAsMaster(port:Int = 8999, conf: Config = defaultConf){

    SmsSystem.startAsMaster(port, isLinux, conf)
    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.nicta.wdy.hdm.coordinator.ClusterExecutorLeader", null)
    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.nicta.wdy.hdm.coordinator.BlockManagerLeader", null)
    SmsSystem.addActor(JOB_RESULT_DISPATCHER, "localhost","org.nicta.wdy.hdm.coordinator.ResultHandler", null)
  }

  def startAsSlave(masterPath:String, port:Int = 10010, conf: Config = defaultConf){
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.nicta.wdy.hdm.coordinator.ClusterExecutorFollower", masterPath+"/"+CLUSTER_EXECUTOR_NAME)
    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.nicta.wdy.hdm.coordinator.BlockManagerFollower", masterPath+"/"+BLOCK_MANAGER_NAME)
    SmsSystem.addActor(JOB_RESULT_DISPATCHER, "localhost","org.nicta.wdy.hdm.coordinator.ResultHandler", null)
  }

  def init(leader:String = "localhost") {

    if(leader == "localhost") {
      startAsMaster()
      leaderPath.set(SmsSystem.rootPath)
    }
    else {
      leaderPath.set(leader)
      startAsSlave(leaderPath.get())
    }
//    scheduler.start()
  }


  def shutdown(){
    SmsSystem.shutDown()
  }



  def explain(hdm:HDM[_, _]) = planer.plan(hdm)

  def compute(hdm:HDM[_, _]):Future[HDM[_,_]] =    {
    addJob(hdm.id, explain(hdm))
  }

  def declareHdm(hdms:Seq[HDM[_,_]]) = {
     SmsSystem.forwardLocalMsg(BLOCK_MANAGER_NAME, AddRefMsg(hdms))
  }

  def addBlock(block:Block[_]) = {
    SmsSystem.forwardLocalMsg(BLOCK_MANAGER_NAME, AddBlockMsg(block))
  }

  def addTask(task:Task[_,_]) = {
    SmsSystem.askAsync(leaderPath.get()+ "/"+CLUSTER_EXECUTOR_NAME, AddTaskMsg(task))
  }

  def addJob(appId:String, hdms:Seq[HDM[_,_]]): Future[HDM[_,_]] = {
    val rootPath =  SmsSystem.rootPath
    val p = SmsSystem.askLocalMsg(JOB_RESULT_DISPATCHER, AddJobMsg(appId, hdms, rootPath + "/"+JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[HDM[_,_]]]
      case none => null
    }
    SmsSystem.askAsync(leaderPath.get()+ "/"+CLUSTER_EXECUTOR_NAME, AddJobMsg(appId, hdms, rootPath + "/"+JOB_RESULT_DISPATCHER))
    if(p ne null) p.future
    else throw new Exception("add job dispatcher failed.")
  }


  def newClusterId():String = {
    UUID.randomUUID().toString
  }

  def newLocalId():String = {
    UUID.randomUUID().toString
  }

}
