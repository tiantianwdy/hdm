package org.nicta.wdy.hdm.coordinator

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import akka.actor.ActorPath
import org.nicta.wdy.hdm.executor.{ParallelTask, HDMContext}
import org.nicta.wdy.hdm.message._
import org.nicta.wdy.hdm.model.HDM
import org.nicta.wdy.hdm.server.{MultiClusterBackend, ServerBackend}

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
                            with DefQueryMsgReceiver
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
      log.info(s"Resource synchronization to [$senderPath] is successful with a state: $state)");
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
      val serializer = hDMContext.defaultSerializer
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
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}]; appId: ${appId}}; res: ${res}  ")
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
