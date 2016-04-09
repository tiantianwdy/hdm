package org.nicta.wdy.hdm.coordinator

import java.nio.ByteBuffer

import akka.actor.ActorPath
import com.baidu.bpit.akka.actors.worker.WorkActor
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.executor.{HDMServerBackend, HDMContext}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.model.{HDMPoJo, AbstractHDM, HDM}

import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 7/09/15.
 */
class HDMClusterLeaderActor(val hdmBackend:HDMServerBackend, val cores:Int) extends WorkActor {

  def this(cores:Int) {
    this(HDMContext.getServerBackend(), cores)
  }

  implicit val executorService: ExecutionContext = HDMContext.executionContext

  def selfPath = self.path.toStringWithAddress(SmsSystem.localAddress).toString

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

    case msg:SchedulingMsg =>
      log.debug(s"received a scheduling msg [${msg}}] ")
      processScheduleMsg(msg)

    case msg:CoordinatingMsg =>
      log.info(s"received a coordination msg [${msg}}] ")
      processClusterMsg(msg)

    case msg:DependencyMsg =>
      log.info(s"received a dependency msg [${msg}}] ")
      processDepMsg(msg)

    case  msg:QueryMsg =>
      log.info(s"received a query msg [${msg}}] ")
      processQueries(msg)

    case x:Any => log.info(s"received a unhanded msg [${x}}] ")
  }


  override def postStop(): Unit = {
    super.postStop()
    hdmBackend.shutdown()
  }

  protected def processQueries: PartialFunction[QueryMsg, Unit] ={
    case msg:ApplicationsQuery =>
      val apps = hdmBackend.dependencyManager.getAllApps()
      val appMap = apps.map{ id =>
        val (app, version) = hdmBackend.dependencyManager.unwrapAppId(id)
        (id , hdmBackend.dependencyManager.getInstanceIds(app, version))
      }.toSeq
      sender() ! ApplicationsResp(appMap)

    case ApplicationInsQuery(app, version) =>
      val instances = hdmBackend.dependencyManager.getInstanceIds(app, version)
      sender() ! ApplicationInsResp(app, version, instances)

    case LogicalFLowQuery(exeId, opt) =>
      val ins = hdmBackend.dependencyManager.getExeIns(exeId)
      val flow = (if(opt) ins.logicalPlanOpt else ins.logicalPlan).map(HDMPoJo(_))
      sender() ! LogicalFLowResp(exeId, flow)

    case ExecutionTraceQuery(execId) =>
      val traces = hdmBackend.dependencyManager.historyManager.getInstanceTraces(execId)
      val resp = ExecutionTraceResp(execId, traces)
      sender() ! resp
  }
  /**
   * handle dependency related msg
   * @return
   */
  protected def processDepMsg: PartialFunction[DependencyMsg, Unit] = {
    case AddApplication(appName, version, content, author) =>
      hdmBackend.submitApplicationBytes(appName, version, content, author)
      hdmBackend.resourceManager.getAllResources().map(_._1) foreach { slave =>
        if (slave != selfPath) {
          log.info(s"sending application [$appName#$version] to $slave ...")
          context.actorSelection(slave) ! AddApplication(appName, version, content, author)
        }
      }

    case AddDependency(appName, version, depName, content, author) =>
      hdmBackend.addDep(appName, version, depName, content, author)
      hdmBackend.resourceManager.getAllResources().map(_._1) foreach { slave =>
        if (slave != selfPath){
          log.info(s"sending dependency [$depName] of [$appName#$version] to $slave ...")
          context.actorSelection(slave) ! AddDependency(appName, version, depName, content, author)
        }
      }
  }

  /**
   * handle coordination messages
   * @return
   */
  protected def processClusterMsg: PartialFunction[CoordinatingMsg, Unit] = {
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      //      if (!followerMap.containsKey(senderPath))
      hdmBackend.resourceManager.addResource(senderPath, state)
      //      if(state > 0)
      //        hdmBackend.resourceManager.release(state)
      log.info(s"A executor has joined from [${senderPath}}] ")

    case LeaveMsg(senderPath) =>
      hdmBackend.resourceManager.removeResource(senderPath)
      log.info(s"A executor has left from [${senderPath}}] ")
  }


  /**
   * handle Scheduling messages
   * @return
   */
  protected def processScheduleMsg: PartialFunction[SchedulingMsg, Unit] = {
    case AddTaskMsg(task) => // task management msg
      hdmBackend.taskReceived(task) onComplete {
        case Success(hdm) => sender ! hdm
        case Failure(t) => sender ! t.toString
      }
      log.info(s"A task has been added from [${sender.path}}]; id: ${task.taskId}} ")

    //deprecated, replaced by SubmitJobMsg
    case AddHDMsMsg(appId, hdms, resultHandler) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appId, "", hdms.head, cores) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")

    /**
     *
     */
    case SerializedJobMsg(appName, version, serHDM, resultHandler, parallel) =>
      val appLoader = hdmBackend.dependencyManager.getClassLoader(appName, version)
      val serializer = HDMContext.defaultSerializer
      val hdm = serializer.deserialize[AbstractHDM[_]](ByteBuffer.wrap(serHDM), appLoader)
      val appId = hdmBackend.dependencyManager.appId(appName, version)
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appName, version, hdm, parallel) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 1, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")

    /**
     * be replaced by SerializedJobMsg
     */
    case SubmitJobMsg(appId, hdm, resultHandler, parallel) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appId, "", hdm, parallel) onComplete {
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
      //      HDMContext.declareHdm(results, false)
      hdmBackend.blockManager.addAllRef(results)
      hdmBackend.resourceManager.incResource(workerPath, 1)
      //      hdmBackend.resourceManager.release(1)
      hdmBackend.taskSucceeded(appId, taskId, func, results)

  }
}

