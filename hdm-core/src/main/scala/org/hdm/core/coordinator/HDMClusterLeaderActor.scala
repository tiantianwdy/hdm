package org.hdm.core.coordinator

import java.nio.ByteBuffer

import akka.actor.ActorPath
import org.hdm.akka.actors.worker.WorkActor
import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor.HDMContext
import org.hdm.core.io.Path
import org.hdm.core.message._
import org.hdm.core.model.{HDMInfo, HDM}
import org.hdm.core.server.ServerBackend

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 7/09/15.
 */
class HDMClusterLeaderActor(val hdmBackend:ServerBackend, val cores:Int , val hDMContext:HDMContext) extends WorkActor {

  def this(cores:Int) {
    this(HDMContext.defaultHDMContext.getServerBackend(), cores, HDMContext.defaultHDMContext)
  }

  implicit val executorService: ExecutionContext = hDMContext.executionContext

  def selfPath = self.path.toStringWithAddress(SmsSystem.localAddress).toString

  override def initParams(params: Any): Int = {
    super.initParams(params)
    //initiate hdm backend server
    hdmBackend.init()
    if(cores > 0){
      hdmBackend.resourceManager.addResource(self.path.toStringWithAddress(SmsSystem.localAddress).toString, cores)
      log.info(s"Leader has been initiated with $cores cores.")
    }
    log.info(s"Leader has been initiated as master only.")
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
      val flow = (if(opt) ins.logicalPlanOpt else ins.logicalPlan).map(HDMInfo(_))
      sender() ! LogicalFLowResp(exeId, flow)

    case PhysicalFlow(exeId) =>
      val ins = hdmBackend.dependencyManager.getExeIns(exeId)
      val flow = ins.physicalPlan.map(HDMInfo(_))
      sender() ! PhysicalFlowResp(exeId, flow)

    case ExecutionTraceQuery(execId) =>
      val traces = hdmBackend.dependencyManager.historyManager.getInstanceTraces(execId)
      val resp = ExecutionTraceResp(execId, traces)
      sender() ! resp

    case AllSlavesQuery(parent) =>
      val cur = System.currentTimeMillis()
      val masterPath = SmsSystem.physicalRootPath
      val masterAddress = Path(masterPath).address
      val nodes = hdmBackend.resourceManager.getAllResources()
        .map { tup =>
        val id = tup._1
        val address = Path(tup._1).address
        NodeInfo(id, "Worker", masterPath, address, cur, tup._2.get(), "Running")
      }
      val masterNode = NodeInfo(masterPath, "Master", null, masterAddress, cur, 0, "Running")
      val resp = if(nodes ne null) nodes.toSeq else Seq.empty[NodeInfo]
      sender() ! AllSLavesResp(resp :+ masterNode)

    case AllAppVersionsQuery() =>
      val apps = hdmBackend.dependencyManager.historyManager.getAllAppIDs()
      val res = mutable.HashMap.empty[String, Seq[String]]
      apps.foreach{ id =>
        val versions = hdmBackend.dependencyManager.historyManager.getAppTraces(id).map(_._1)
        res += id -> versions
//        val (app, version) = hdmBackend.dependencyManager.unwrapAppId(id)
//        res.getOrElseUpdate(app, mutable.Buffer.empty[String]) += version
      }
      sender() ! AllAppVersionsResp(res.toSeq)

    case DependencyTraceQuery(app, version) =>
      val traces = hdmBackend.dependencyManager.historyManager.getAppTraces(app)
      val res = if(version == null || version.isEmpty) {
        traces
      } else { // find the versions equal or larger then version
        val versionCode = version.replaceAll(".", "").toInt
        traces.filter(tup => tup._1.replaceAll(".", "").toInt >= versionCode)
      }
      sender() ! DependencyTraceResp(app, version, res)
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
 *
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

    case LeaveMsg(nodes) =>
      nodes foreach {node =>
        hdmBackend.resourceManager.removeResource(node)
      }
      log.info(s"Executors have left the cluster from [${nodes}}] ")
  }


  /**
   * handle Scheduling messages
 *
   * @return
   */
  protected def processScheduleMsg: PartialFunction[SchedulingMsg, Unit] = {
    case AddTaskMsg(task) => // task management msg
      hdmBackend.taskReceived(task) onComplete {
        case Success(hdm) => sender ! hdm
        case Failure(t) => sender ! t.toString
      }
      log.info(s"A task has been added from [${sender.path}}]; id: ${task.taskId}} ")

    /**
     * deprecated, replaced by SerializedJobMsg
     *
     */
    case AddHDMsMsg(appId, hdms, resultHandler) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appId, "", hdms.head, cores) onComplete {
        case Success(hdm) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(appId, 0, hdm)
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")

    /**
     * process a job msg with serialized hdm object
     */
    case SerializedJobMsg(appName, version, serHDM, resultHandler, from, parallel) =>
      val appLoader = hdmBackend.dependencyManager.getClassLoader(appName, version)
      val serializer = HDMContext.JOB_SERIALIZER
      val hdm = serializer.deserialize[HDM[_]](ByteBuffer.wrap(serHDM), appLoader)
      val appId = hdmBackend.dependencyManager.appId(appName, version)
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appName, version, hdm, parallel) onComplete {
        case Success(res) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(hdm.id, 0, res)
          val scheduleTime = hdmBackend.scheduler.totalScheduleTime.getAndSet(0L)
          log.info(s"A job has completed successfully with scheduling time: ${scheduleTime} ms. result has been send to [${resultHandler}}]; appId: ${appId}}.")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}}]; id: ${appId}} ")

    /**
     * has been replaced by SerializedJobMsg
     */
    case SubmitJobMsg(appId, hdm, resultHandler, parallel) =>
      val senderPath = sender.path
      val fullPath = ActorPath.fromString(resultHandler).toStringWithAddress(senderPath.address)
      hdmBackend.jobReceived(appId, "", hdm, parallel) onComplete {
        case Success(res) =>
          val resActor = context.actorSelection(fullPath)
          resActor ! JobCompleteMsg(hdm.id, 0, res)
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

