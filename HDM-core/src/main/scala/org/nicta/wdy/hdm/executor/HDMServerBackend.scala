package org.nicta.wdy.hdm.executor

import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorPath
import com.baidu.bpit.akka.actors.worker.WorkActor
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.planing.HDMPlaner
import org.nicta.wdy.hdm.scheduling.Scheduler
import org.nicta.wdy.hdm.storage.HDMBlockManager

import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Promise, Future}
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 24/08/15.
 */
class HDMServerBackend(val appManager: AppManager,
                       val blockManager: HDMBlockManager,
                       val scheduler: Scheduler,
                       val planner: HDMPlaner,
                       val resourceManager: ResourceManager,
                       val eventManager: PromiseManager) {




  def init(): Unit = {
    scheduler.init()
    new Thread{
      override def run(): Unit = {
        scheduler.startup()
      }
    }.start()
  }

  def jobReceived(jobId:String, hdm:HDM[_,_], parallelism:Int):Future[_] ={
    appManager.addApp(jobId, hdm)
    val plan = HDMContext.explain(hdm, parallelism)
    appManager.addPlan(jobId, plan)
    scheduler.submitJob(jobId, plan)
  }


  def taskReceived[I, R](task:Task[_,_]):Future[_] = {
    val promise = Promise[HDM[I, R]]()
    eventManager.addPromise(task.taskId, promise)
    scheduler.addTask(task)
    promise.future
  }


/*  def jobCancelled():Future[_]


  def explain():Future[_]


  def jobCompleted(jobId:String, res:HDM[_,_])*/


  def taskSucceeded(appId:String, taskId:String, func:String, results:Seq[HDM[_,_]]): Unit ={
    scheduler.taskSucceeded(appId,taskId, func, results.map(_.toURL))
  }


  def shutdown() ={
    scheduler.stop()
  }

}


class HDMClusterLeaderActor(val hdmBackend:HDMServerBackend, val cores:Int) extends WorkActor {

  def this(cores:Int) {
    this(HDMContext.getServerBackend(), cores)
  }

  implicit val executorService: ExecutionContext = HDMContext.executionContext


  override def initParams(params: Any): Int = {
    super.initParams(params)
    //initiate hdm backend server
    hdmBackend.init()
    hdmBackend.resourceManager.addResource(self.path.toStringWithAddress(SmsSystem.localAddress).toString, cores)
    log.info(s"Leader has been initiated with $cores cores.")
    1
  }

  /**
   *
   * process business message
   */
  override def process: PartialFunction[Any, Unit] = {
    // task management msg
    case AddTaskMsg(task) =>
      hdmBackend.taskReceived(task) onComplete {
        case Success(hdm) => sender ! hdm
        case Failure(t) => sender ! t.toString
      }
      log.info(s"A task has been added from [${sender.path}}]; id: ${task.taskId}} ")

    //deprecated, replaced by SubmitJobMsg
    case AddHDMsMsg(appId, hdms, resultHandler) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appId, hdms.head, cores) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")


    case SubmitJobMsg(appId, hdm, resultHandler, parallel) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appId, hdm, parallel) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")



    case TaskCompleteMsg(appId, taskId, func, results) =>
      log.info(s"received a task completed msg: ${taskId + "_" + func}")
      val workerPath = sender.path.toString
      HDMContext.declareHdm(results, false)
      hdmBackend.resourceManager.incResource(workerPath, 1)
      hdmBackend.resourceManager.release(1)
      hdmBackend.taskSucceeded(appId, taskId, func, results)

    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      //      if (!followerMap.containsKey(senderPath))
      hdmBackend.resourceManager.addResource(senderPath, state)
      if(state > 0)
        hdmBackend.resourceManager.release(state)
      log.info(s"A executor has joined from [${senderPath}}] ")

    case LeaveMsg(senderPath) =>
      hdmBackend.resourceManager.removeResource(senderPath)
      log.info(s"A executor has left from [${senderPath}}] ")

    case x => unhandled(x); log.info(s"received a unhanded msg [${x}}] ")
  }


  override def postStop(): Unit = {
    super.postStop()
    hdmBackend.shutdown()
  }

}

