package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.planing.{FunctionFusion, StaticPlaner}

import scala.concurrent.{Promise, Future}
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.functions.{ParallelFunction, DDMFunction_1, SerializableFunction}
import org.nicta.wdy.hdm.storage.{Block, HDMBlockManager}
import java.util.concurrent.atomic.AtomicReference
import org.nicta.wdy.hdm.server.HDMServer
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.message._
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

  lazy val defaultConf = ConfigFactory.load("hdm-core.conf")

  val CORES = Runtime.getRuntime.availableProcessors

  val isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  val blockManager = HDMBlockManager()

  val planer = StaticPlaner

  val scheduler = new SimpleFIFOScheduler

  val leaderPath: AtomicReference[String] = new AtomicReference[String]()

  val CLUSTER_EXECUTOR_NAME:String =  "ClusterExecutor"

  val BLOCK_MANAGER_NAME:String =  "BlockManager"

  val JOB_RESULT_DISPATCHER:String = "ResultDispatcher"

  def clusterBlockPath = leaderPath.get() + "/" + BLOCK_MANAGER_NAME

  def localBlockPath = SmsSystem.physicalRootPath + "/" + BLOCK_MANAGER_NAME

  def startAsMaster(port:Int = 8999, conf: Config = defaultConf, cores:Int = 0){

    SmsSystem.startAsMaster(port, isLinux, conf)
    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.nicta.wdy.hdm.coordinator.ClusterExecutorLeader", cores)
    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.nicta.wdy.hdm.coordinator.BlockManagerLeader", null)
    SmsSystem.addActor(JOB_RESULT_DISPATCHER, "localhost","org.nicta.wdy.hdm.coordinator.ResultHandler", null)
    leaderPath.set(SmsSystem.rootPath)
  }

  def startAsSlave(masterPath:String, port:Int = 10010, conf: Config = defaultConf, cores:Int = CORES){
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.nicta.wdy.hdm.coordinator.ClusterExecutorFollower", masterPath+"/"+CLUSTER_EXECUTOR_NAME)
    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.nicta.wdy.hdm.coordinator.BlockManagerFollower", masterPath+"/"+BLOCK_MANAGER_NAME)
    SmsSystem.addActor(JOB_RESULT_DISPATCHER, "localhost","org.nicta.wdy.hdm.coordinator.ResultHandler", null)
    leaderPath.set(masterPath)
  }

  def startAsClient(masterPath:String, port:Int = 20010, conf: Config = defaultConf, localExecution:Boolean = false){
    SmsSystem.startAsSlave(masterPath, port, isLinux, conf)
    SmsSystem.addActor(BLOCK_MANAGER_NAME, "localhost","org.nicta.wdy.hdm.coordinator.BlockManagerFollower", masterPath+"/"+BLOCK_MANAGER_NAME)
    SmsSystem.addActor(JOB_RESULT_DISPATCHER, "localhost","org.nicta.wdy.hdm.coordinator.ResultHandler", null)
    if(localExecution)
      SmsSystem.addActor(CLUSTER_EXECUTOR_NAME, "localhost","org.nicta.wdy.hdm.coordinator.ClusterExecutorFollower", masterPath+"/"+CLUSTER_EXECUTOR_NAME)
    leaderPath.set(masterPath)
  }

  def init(leader:String = "localhost", cores:Int = CORES) {

    if(leader == "localhost") {
      startAsMaster(cores = cores)
    } else {
      if(cores > 0)
        startAsClient(masterPath = leader, localExecution = true)
      else
        startAsClient(masterPath = leader, localExecution = false)
    }
//    scheduler.start()
  }


  def shutdown(){
    SmsSystem.shutDown()
  }



  def explain(hdm:HDM[_, _],parallelism:Int) = {
    val hdmOpt = new FunctionFusion().optimize(hdm)
    planer.plan(hdmOpt, parallelism)
//    val hdms = planer.plan(hdm, parallelism)
//    hdmsOpt
  }

  def compute(hdm:HDM[_, _], parallelism:Int):Future[HDM[_,_]] =    {
    addJob(hdm.id, explain(hdm, parallelism))
  }

  def declareHdm(hdms:Seq[HDM[_,_]]) = {
     SmsSystem.forwardLocalMsg(BLOCK_MANAGER_NAME, AddRefMsg(hdms))
  }

  def addBlock(block:Block[_]) = {
    SmsSystem.forwardLocalMsg(BLOCK_MANAGER_NAME, AddBlockMsg(block))
  }

  def queryBlock(id:String, location:String) = {
    SmsSystem.forwardLocalMsg(BLOCK_MANAGER_NAME, QueryBlockMsg(id, location))
  }

  def addTask(task:Task[_,_]) = {
    SmsSystem.askAsync(leaderPath.get()+ "/"+CLUSTER_EXECUTOR_NAME, AddTaskMsg(task))
  }

  def addJob(appId:String, hdms:Seq[HDM[_,_]]): Future[HDM[_,_]] = {
    val rootPath =  SmsSystem.rootPath
    HDMContext.declareHdm(hdms)
    val p = SmsSystem.askLocalMsg(JOB_RESULT_DISPATCHER, AddJobMsg(appId, hdms, rootPath + "/"+JOB_RESULT_DISPATCHER)) match {
      case Some(promise) => promise.asInstanceOf[Promise[HDM[_,_]]]
      case none => null
    }
    SmsSystem.askAsync(leaderPath.get()+ "/"+CLUSTER_EXECUTOR_NAME, AddJobMsg(appId, hdms, rootPath + "/"+JOB_RESULT_DISPATCHER))
    if(p ne null) p.future
    else throw new Exception("add job dispatcher failed.")
  }

  def clean(appId:String): Unit ={
    //todo clean all the resources used by this application
  }


  def newClusterId():String = {
    UUID.randomUUID().toString
  }

  def newLocalId():String = {
    UUID.randomUUID().toString
  }

}
