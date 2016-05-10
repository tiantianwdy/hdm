package org.nicta.wdy.hdm.coordinator

import java.nio.ByteBuffer

import akka.actor.ActorPath
import com.baidu.bpit.akka.server.SmsSystem
import org.nicta.wdy.hdm.io.Path
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.model.{HDM, HDMPoJo}

import scala.collection.mutable
import scala.util.{Failure, Success}

/**
 * Created by tiantian on 9/05/16.
 */
trait DefQueryMsgReceiver extends QueryReceiver{

  this: AbstractHDMLeader =>

  override def processQueries: PartialFunction[QueryMsg, Unit] = {
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

    case PhysicalFlow(exeId) =>
      val ins = hdmBackend.dependencyManager.getExeIns(exeId)
      val flow = ins.physicalPlan.map(HDMPoJo(_))
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

}

trait DefDepReceiver extends DepMsgReceiver {

  this: AbstractHDMLeader =>

  override def processDepMsg: PartialFunction[DependencyMsg, Unit] = {
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

}

trait DefSchedulingReceiver extends SchedulingMsgReceiver {

  this: AbstractHDMLeader =>

  override def processScheduleMsg: PartialFunction[SchedulingMsg, Unit] = {
    case AddTaskMsg(task) => // task management msg
      hdmBackend.taskReceived(task) onComplete {
        case Success(hdm) => sender ! hdm
        case Failure(t) => sender ! t.toString
      }
      log.info(s"A task has been added from [${sender.path}}]; id: ${task.taskId}} ")


    /**
     * process a job msg with serialized hdm object
     */
    case SerializedJobMsg(appName, version, serHDM, resultHandler, parallel) =>
      val appLoader = hdmBackend.dependencyManager.getClassLoader(appName, version)
      val serializer = hDMContext.defaultSerializer
      val hdm = serializer.deserialize[HDM[_]](ByteBuffer.wrap(serHDM), appLoader)
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

trait SingleClusteringReceiver extends ClusterMsgReceiver {

  this: AbstractHDMLeader =>

  override def processClusterMsg: PartialFunction[CoordinatingMsg, Unit] = {
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      //      if (!followerMap.containsKey(senderPath))
      hdmBackend.resourceManager.addResource(senderPath, state)
      log.info(s"A executor has joined from [${senderPath}}] ")

    case LeaveMsg(senderPath) =>
      hdmBackend.resourceManager.removeResource(senderPath)
      log.info(s"A executor has left from [${senderPath}}] ")
  }
}
