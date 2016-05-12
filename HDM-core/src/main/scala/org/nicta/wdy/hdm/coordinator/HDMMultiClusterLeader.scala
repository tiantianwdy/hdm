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
                            with DefDepReceiver{

  //  holding the task map to the origins of the remote tasks received from sibling masters
  private val remoteTaskMap = new ConcurrentHashMap[String, String]()

  def this(cores: Int) {
    this(HDMContext.defaultHDMContext.getServerBackend("multiple").asInstanceOf[MultiClusterBackend], cores, HDMContext.defaultHDMContext)
  }


  override def processClusterMsg: PartialFunction[CoordinatingMsg, Unit] = {
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      hdmBackend.resourceManager.addResource(senderPath, state)
      hdmBackend.resourceManager.addChild(senderPath, state)
      log.info(s"A child has joined from [${senderPath}] ")

    case LeaveMsg(senderPath) =>
      hdmBackend.resourceManager.removeResource(senderPath)
      hdmBackend.resourceManager.removeChild(senderPath)
      hdmBackend.resourceManager.removeSibling(senderPath)
      log.info(s"A node has left from [${senderPath}] ")

    case CollaborateMsg(path, state) =>
      val senderPath = sender().path.toString
      hdmBackend.resourceManager.addResource(senderPath, state)
      hdmBackend.resourceManager.addSibling(senderPath, state)
      log.info(s"A sibling has joined from [${senderPath}] ")

    case AskCollaborateMsg(sibling) =>
      val totalCores = hdmBackend.resourceManager.getAllChildrenCores()
      hdmBackend.resourceManager.addSibling(sibling, 0)
      context.actorSelection(sibling) ! CollaborateMsg(selfPath, totalCores)
  }

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
          log.info(s"A job has completed successfully. result has been send to [${resultHandler}]; appId: ${appId}} ")
        case Failure(t) =>
          context.actorSelection(resultHandler) ! JobCompleteMsg(appId, 1, t.toString)
          log.info(s"A job has failed. result has been send to [${resultHandler}]; appId: ${appId}} ")
      }
      log.info(s"A job has been added from [${sender.path}]; id: ${appId} ")



    case TaskCompleteMsg(appId, taskId, func, results) =>
      log.info(s"received a task completed msg: ${taskId + "_" + func}")
      val workerPath = sender.path.toString
      hdmBackend.blockManager.addAllRef(results)
      hdmBackend.resourceManager.incResource(workerPath, 1)
      if(remoteTaskMap.containsKey(taskId)){
        hdmBackend.resourceManager.siblingMap.foreach(kv =>
          if(kv._1 != workerPath) context.actorSelection(kv._1) ! TaskCompleteMsg(appId, taskId, func, results)
        )
      } else {
        hdmBackend.taskSucceeded(appId, taskId, func, results)
      }

    case SerializedTaskMsg(appName, version, taskId, serTask) => // receive task from other master
      log.info(s"received a SerializedTaskMsg from: ${sender().path}")
      remoteTaskMap.put(taskId, sender().path.toString)
      val loader = hdmBackend.dependencyManager.getClassLoader(appName, version)
      val task = hDMContext.defaultSerializer.deserialize[ParallelTask[_]](ByteBuffer.wrap(serTask), loader)
      hdmBackend.scheduler.scheduleRemoteTask(task)
  }

}

trait MultiClusterReceiver extends ClusterMsgReceiver {

  this: AbstractHDMLeader =>

  override def processClusterMsg: PartialFunction[CoordinatingMsg, Unit] = {
    // coordinating msg
    case JoinMsg(path, state) =>
      val senderPath = sender().path.toString
      hdmBackend.resourceManager.addResource(senderPath, state)
      log.info(s"A executor has joined from [${senderPath}] ")

    case LeaveMsg(senderPath) =>
      hdmBackend.resourceManager.removeResource(senderPath)
      log.info(s"A executor has left from [${senderPath}] ")

    case CollaborateMsg(path, state) =>
      val senderPath = sender().path.toString
      hdmBackend.resourceManager.addResource(senderPath, state)
      //todo add sibling to [[TreeResourceManager]]
      log.info(s"A executor has joined from [${senderPath}] ")
  }
}


trait MultiClusterScheduling extends SchedulingMsgReceiver {

  this: AbstractHDMLeader =>

  override def processScheduleMsg: PartialFunction[SchedulingMsg, Unit] = ???
}
