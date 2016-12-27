package org.hdm.akka.actors

import org.hdm.akka.extensions.{CachedDataService, CachedDataQueryService, CachedDataExtensionProvider, MasterExtensionImpl}
import org.hdm.akka.messages._

import akka.actor.Actor
import akka.actor.ActorRef
import org.hdm.akka.server.SmsSystem
import org.hdm.akka.logEx

/**
 * master节点负责管理和维护整个集群的状态，主要的处理逻辑由[[org.hdm.akka.messages.MasterSlaveProtocol]] 实现
 * master actor is responsible to  manage (create and initiate) root slave actors and each actor created through master system.
 * it  also receives the heart-beats from slaves and monitor their states.
 * @author wudongyao
 * @date 2013-7-11
 * @version 0.0.1
 *
 */
class MasterActor(override var persistenceActor: ActorRef) extends Actor with MasterExtensionImpl with MasterQueryExtensionImpl {

  override def preStart() {
    log.info("MasterActor starting at:" + context.self)
  }

  def receive = {
    case msg: MasterSlaveProtocol => try {
      log.debug(s"received a master-slave msg: $msg")
      handleMasterSlaveMsg(msg)
    } catch logEx(log, self.path.toString)
    case msg: RoutingProtocol => try {
      log.debug(s"received a routing msg: $msg")
      handleRootingMsg(msg)
    } catch logEx(log, self.path.toString)
    case query: QueryProtocol => try {
      log.debug(s"received a query msg: $query")
      val reply = handleQueryMsg(query)
      sender ! reply
      log.debug(s"send a reply msg: $reply")
    } catch logEx(log, self.path.toString)
    case msg: Any => log.warning("unhandled msg:" + msg + "from " + sender.path); unhandled(msg)
  }

  override def postStop() {
    log.info("MasterActor stopped at:" + context.self)
  }

}

trait MasterQueryExtensionImpl extends QueryExtension {

  this: Actor =>

  val cachedDataService = new CachedDataQueryService {}

  override def handleQueryMsg(msg: QueryProtocol): Option[Reply] = msg match {
    case Query(op, prop, key, source, _, _) => op match {
      case "smsSystem/allSlaves" => allSlaves(prop, key)
      case "smsSystem/allActors" => allActors(prop, key)
      case "smsSystem/children" => allChildren(prop, key)
      case op: String if (!op.isEmpty) => cachedDataService.handleQueryMsg(msg)
      case _ => None
    }
    case _ => None

  }

  def allSlaves(prop: String, key: String): Option[Reply] = Some(Reply(result = SmsSystem.allSlaves()))

  def allActors(prop: String, key: String): Option[Reply] = Some(Reply(result = SmsSystem.listAllRooting()))

  def allChildren(prop: String, key: String): Option[Reply] = Some(Reply(result = SmsSystem.children(key)))

}
