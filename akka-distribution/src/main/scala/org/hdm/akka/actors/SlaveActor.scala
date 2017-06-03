package org.hdm.akka.actors

import org.hdm.akka.extensions.RoutingService
import org.hdm.akka.extensions.SlaveExtensionImpl
import org.hdm.akka.messages.JoinMsg
import org.hdm.akka.messages.MasterSlaveProtocol
import org.hdm.akka.messages.RoutingProtocol
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.actorRef2Scala
import akka.actor.ActorSelection
import org.hdm.akka._
import messages.JoinMsg

/**
 * slaveActor 负责维护本节点的actor管理和状态维护，并与master节点进行通信接收集群管理消息
 * 主要的处理逻辑实现在 @link SlaveExtensionImpl
 * @author wudongyao
 * @date 2013-7-11
 * @version
 *
 */
class SlaveActor(var masterPath: String) extends Actor with SlaveExtensionImpl with RoutingService {

  def this(master: ActorRef) {
    this(if (master != null) master.path.toString else null)
  }

  /**
   * 启动时向master发送Join消息
   */
  override def preStart() {
    log.info("SlaveActor starting at:" + context.self)
    if (masterPath ne null)
      context.actorSelection(masterPath) ! JoinMsg()
  }

  /**
   * 消息处理逻辑，主要通过 MasterSlaveProtocol ，RootingProtocol进行与master进行通信
   * @return  unit
   */
  def receive = {
    case msg: MasterSlaveProtocol => try {
      log.info("received a message:" + msg.getClass)
      handleMasterSlaveMsg(msg)
    } catch logEx(log, self.path.toString)
    case msg: RoutingProtocol => log.info(s"received a rooting msg: $msg");handleRootingMsg(msg)
    case xm => unhandled(xm);log.warning("unhandled message type.{}", xm)
  }

  override def postStop() {
    super.postStop()
    log.info("Actor:" + context.self + "has been stopped")
  }

}

object SlaveActor {
  def apply(masterPath: String = null) = {
    new SlaveActor(masterPath)
  }
}