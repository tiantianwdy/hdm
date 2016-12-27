package org.hdm.akka.actors.worker

import org.hdm.akka.configuration.Deployment
import org.hdm.akka.configuration.Parameters
import org.hdm.akka.extensions.{RoutingExtension, RoutingService, WorkActorExtension}
import org.hdm.akka.messages.InitMsg
import org.hdm.akka.messages.StateMsg
import org.hdm.akka.messages.JoinMsg
import org.hdm.akka.messages.MasterSlaveProtocol
import org.hdm.akka.messages.StopMsg
import org.hdm.akka.messages.SuspendMsg
import org.hdm.akka.messages.UpdateMsg
import org.hdm.akka.monitor.ActorCounting
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.actorRef2Scala
import org.hdm.akka.messages.MasterSlaveProtocol

/**
 * 所有工作节点的基类，继承了counting, logging, 和Rooting的Trait
 * basic class of all work actors extending with traits [ActorCounting] and [ActorLogging]
 * this actor handles the basic communication for master messages @link MasterSlaveProtocol
 * @author wudongyao
 * @date 2013-7-2 
 * @version 0.0.1
 *
 */
abstract class WorkActor(params: Any) extends Actor with WorkActorExtension with ActorCounting with ActorLogging with RoutingService {

  import context._


  def receive = {
    case msg: MasterSlaveProtocol => handleMasterSlaveMsg(msg)
    case xm => unhandled(xm); log.warning(s"received a unhandled message:$xm")
    }

  override def preStart() {
    log.info("Actor starting at:{}", context.self)
  }


  override def postStop() {
    for (child <- context.children) {
      context.stop(child)
    }
    context.stop(self)
    log.info(s"Actor: ${self.toString()} has been stopped.")
    stateChanged(Deployment.UN_DEPLOYED,"actor has been stopped.")
    log.info(s"Actor: ${context.self} has been stopped")
  }
}

