package org.hdm.core.coordinator

import java.nio.ByteBuffer
import java.util.concurrent.{TimeUnit, ConcurrentHashMap}

import akka.actor.ActorPath
import akka.util.Timeout
import org.hdm.akka.server.SmsSystem
import org.hdm.core.executor.{ParallelTask, HDMContext}
import org.hdm.core.io.Path
import org.hdm.core.message._
import org.hdm.core.model.{HDMPoJo, HDM}
import org.hdm.core.server.{MultiClusterBackend, ServerBackend}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.collection.JavaConversions._

/**
 * Created by tiantian on 9/05/16.
 */

/**
 * a class leader which is able to handle multi-clusters with sibling master nodes for each master
 * @param hdmBackend
 * @param cores
 * @param hDMContext
 */
class HDMMultiClusterLeader(override val hdmBackend:MultiClusterBackend,
                            override val cores:Int ,
                            override val hDMContext:HDMContext)
                            extends AbstractHDMLeader(hdmBackend, cores, hDMContext)
                            with MultiClusterQueryReceiver
                            with MultiCLusterDepReceiver
                            with MultiClusterReceiver
                            with MultiClusterScheduling {


  def this(cores: Int) {
    this(HDMContext.defaultHDMContext.getServerBackend("multiple").asInstanceOf[MultiClusterBackend], cores, HDMContext.defaultHDMContext)
  }

}

/**
 * the message receiver for processing cluster coordination messages
 */
trait MultiClusterReceiver extends ClusterMsgReceiver {

  this: HDMMultiClusterLeader =>

  def syncRes(): Unit ={
    hdmBackend.resourceManager.getSiblingRes() foreach { res =>
      val nValue = hdmBackend.resourceManager.getAllChildrenCores()
      context.actorSelection(res._1) ! ResSync(resId = selfPath, change = nValue)
    }
  }

  override def processClusterMsg: PartialFunction[CoordinatingMsg, Unit] = {
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      //      hdmBackend.resourceManager.addResource(senderPath, state)
      hdmBackend.resourceManager.addChild(senderPath, state)
      log.info(s"A child has joined from [${senderPath}] ")
      syncRes()

    case LeaveMsg(senderPath) =>
      hdmBackend.resourceManager.removeResource(senderPath)
      log.info(s"A node has left from [${senderPath}] ")
      syncRes()

    case CollaborateMsg(path, state) =>
      val senderPath = sender().path.toString
      hdmBackend.resourceManager.addSibling(senderPath, state)
      log.info(s"A sibling has joined from [${senderPath}] ")

    case AskCollaborateMsg(sibling) =>
      val totalCores = hdmBackend.resourceManager.getAllChildrenCores()
      hdmBackend.resourceManager.addSibling(sibling, 0)
      context.actorSelection(sibling) ! CollaborateMsg(selfPath, totalCores)


    case ResSync(msgId, resId, change) =>
      val senderPath = sender().path.toString
      hdmBackend.resourceManager.addSibling(resId, change)
      context.actorSelection(senderPath) ! ResSyncResp(msgId, 0)

    case ResSyncResp(msgId, state) =>
      val senderPath = sender().path.toString
      log.info(s"Resource synchronization to [$senderPath] is successful with a state: $state)")

    case MigrationMsg(workerPath, toMaster) =>
      val res = hdmBackend.resourceManager.getResource(workerPath)
      hdmBackend.resourceManager.removeResource(workerPath)
      log.info(s"A node has left from [${workerPath}] ")
      syncRes()
      //send msg to the worker to ask it join the new master
      context.actorSelection(workerPath) ! MigrationMsg(workerPath, toMaster)

  }
}

/**
 * the message receiver for processing dependencies messages
 */
trait MultiCLusterDepReceiver extends DepMsgReceiver {

  this: HDMMultiClusterLeader =>

  override def processDepMsg: PartialFunction[DependencyMsg, Unit] = {

    case AddApplication(appName, version, content, author) =>
      hdmBackend.submitApplicationBytes(appName, version, content, author)
      hdmBackend.resourceManager.getChildrenRes().map(_._1) foreach { slave =>
        if (slave != selfPath) {
          log.info(s"sending application [$appName#$version] to $slave ...")
          context.actorSelection(slave) ! AddApplication(appName, version, content, author)
        }
      }

    case AddDependency(appName, version, depName, content, author) =>
      hdmBackend.addDep(appName, version, depName, content, author)
      hdmBackend.resourceManager.getChildrenRes().map(_._1) foreach { slave =>
        if (slave != selfPath){
          log.info(s"sending dependency [$depName] of [$appName#$version] to $slave ...")
          context.actorSelection(slave) ! AddDependency(appName, version, depName, content, author)
        }
      }
  }

}


/**
 * Handler for processing scheduling messages in a multi-cluster leader
 */
trait MultiClusterScheduling extends SchedulingMsgReceiver {


  this: HDMMultiClusterLeader =>

  //  holding the task map to the origins of the remote tasks received from sibling masters
  protected val remoteTaskMap = new ConcurrentHashMap[String, String]()

  override def processScheduleMsg: PartialFunction[SchedulingMsg, Unit] = {

    case AddTaskMsg(task) => // task management msg
      hdmBackend.taskReceived(task) onComplete {
        case Success(hdm) => sender ! hdm
        case Failure(t) => sender ! t.toString
      }
      log.info(s"A task has been added from [${sender.path}]; id: ${task.taskId}} ")


    /**
     * process a job msg with serialized hdm object
     */
    case SerializedJobMsg(appName, version, serHDM, resultHandler, from, parallel) =>
      val appLoader = hdmBackend.dependencyManager.getClassLoader(appName, version)
      val serializer = HDMContext.JOB_SERIALIZER
      val hdm = serializer.deserialize[HDM[_]](ByteBuffer.wrap(serHDM), appLoader)
      val appId = hdmBackend.dependencyManager.appId(appName, version)
      val siblings = hdmBackend.resourceManager.getSiblingRes()
      val future = if(siblings.containsKey(from)){
        log.info(s"received a coordination job from: $from")
        hdmBackend.coordinationJobReceived(appName, version, hdm, parallel)
      } else {
        log.info(s"received a regular job from: $from")
        hdmBackend.jobReceived(appName, version, hdm, parallel)
      }
      future onComplete {
        case Success(resp) =>
          val res = resp match {
            case hdm:HDM[_] => hdm.toSerializable()
            case other => other
          }
          val resActor = context.actorSelection(resultHandler)
          resActor ! JobCompleteMsg(hdm.id, 0, res)
          val scheduleTime = hdmBackend.scheduler.totalScheduleTime.getAndSet(0L)
          log.info(s"A job has completed successfully with scheduling time: ${scheduleTime} ms. result has been send to [${resultHandler}}]; appId: ${appId}}.")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(hdm.id, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}]; id: ${appId} ")



    case TaskCompleteMsg(appId, taskId, func, results) =>
      log.info(s"received a task completed msg: ${taskId + "_" + func}")
      val workerPath = sender.path.toString
      hdmBackend.blockManager.addAllRef(results)
      hdmBackend.resourceManager.incResource(workerPath, 1)
      if(remoteTaskMap.containsKey(taskId)){
      // if the completed task is a remote task forward the msg to siblings
        hdmBackend.resourceManager.siblingMap.foreach(kv =>
          if(kv._1 != workerPath) {
            log.info(s"sending a remote TaskCompleteMsg: ${taskId + "_" + func} to ${kv._1}")
            context.actorSelection(kv._1) ! TaskCompleteMsg(appId, taskId, func, results)
          }
        )
      } else {
        log.info(s"Local task completed: ${taskId + "_" + func}")
        hdmBackend.taskSucceeded(appId, taskId, func, results)
      }

    case SerializedTaskMsg(appName, version, taskId, serTask) => // received task from other masters
      log.info(s"received a SerializedTaskMsg from: ${sender().path}")
      remoteTaskMap.put(taskId, sender().path.toString)
      val loader = hdmBackend.dependencyManager.getClassLoader(appName, version)
      val task = hDMContext.defaultSerializer.deserialize[ParallelTask[_]](ByteBuffer.wrap(serTask), loader)
      hdmBackend.scheduler.scheduleRemoteTask(task)

  }


}

import akka.pattern.ask
import concurrent.duration.{Duration, DurationInt}

trait MultiClusterQueryReceiver extends QueryReceiver {

  this: HDMMultiClusterLeader =>

  implicit val maxWaitResponseTime = Duration(300, TimeUnit.SECONDS)
  implicit val timeout = Timeout(300 seconds)

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
      if(ins ne null) {
        val flow = (if(opt) ins.logicalPlanOpt else ins.logicalPlan).map(HDMPoJo(_))
        sender() ! LogicalFLowResp(exeId, flow)
      }

    case PhysicalFlow(exeId) =>
      val ins = hdmBackend.dependencyManager.getExeIns(exeId)
      if(ins ne null) {
        val flow = ins.physicalPlan.map(HDMPoJo(_))
        sender() ! PhysicalFlowResp(exeId, flow)
      }

    case ExecutionTraceQuery(execId) =>
      val traces = hdmBackend.dependencyManager.historyManager.getInstanceTraces(execId)
      val resp = ExecutionTraceResp(execId, traces)
      sender() ! resp

    case AllSlavesQuery(parent) =>
      val cur = System.currentTimeMillis()
      val masterPath = self.path.toStringWithAddress(SmsSystem.localAddress).toString
      val masterAddress = Path(masterPath).address
      val nodes = hdmBackend.resourceManager.getChildrenRes()
        .map { tup =>
        val id = tup._1
        val address = Path(tup._1).address
        NodeInfo(id, "Worker", masterPath, address, cur, tup._2.get(), "Running")
      }
      log.info(s" cluster nodes number: ${nodes}")
      val siblings = hdmBackend.resourceManager.getSiblingRes()
      val siblingNodes = siblings.map{ tup =>
        val id = tup._1
        val address = Path(tup._1).address
        NodeInfo(id, "Master", masterPath, address, cur, tup._2.get(), "Running")
      }
      log.info(s" cluster sibling nodes number: ${siblingNodes}")
      val masterNode = NodeInfo(masterPath, "Master", null, masterAddress, cur, 0, "Running")
      //foreach sibling send msg to get slaves
      val siblingChildren = mutable.Buffer.empty[NodeInfo]
      siblings.foreach{ tup =>
        val future = context.actorSelection(tup._1) ? DescendantQuery(tup._1)
        Await.result[Any](future, maxWaitResponseTime) match {
          case AllSLavesResp(nodes) =>
            siblingChildren ++= nodes

          case other:Any => // do nothing
        }
      }
      log.info(s" cluster sibling children number: ${siblingChildren}")
      val resp = if(nodes ne null) nodes.toSeq ++ siblingNodes ++ siblingChildren else Seq.empty[NodeInfo]
      sender() ! AllSLavesResp(resp :+ masterNode)

    case DescendantQuery(parent) => // return only children of the master
      val cur = System.currentTimeMillis()
      val masterPath = self.path.toStringWithAddress(SmsSystem.localAddress).toString
      val nodes = hdmBackend.resourceManager.getChildrenRes()
        .map { tup =>
        val id = tup._1
        val address = Path(tup._1).address
        NodeInfo(id, "Worker", masterPath, address, cur, tup._2.get(), "Running")
      }
      val resp = if(nodes ne null) nodes.toSeq else Seq.empty[NodeInfo]
      sender() ! AllSLavesResp(resp)


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