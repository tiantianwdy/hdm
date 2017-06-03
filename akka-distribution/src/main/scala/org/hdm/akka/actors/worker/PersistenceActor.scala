package org.hdm.akka.actors.worker

import akka.actor.Actor
import org.hdm.akka.messages.BasicMessage
import org.hdm.akka.messages.HeartbeatMsg
import akka.event.slf4j.Logger
import akka.event.Logging
import org.hdm.akka.persistence.PersistenceService
import org.hdm.akka.messages.MasterSlaveProtocol


/**
 * wudongyao
 */
class PersistenceActor(val persistenceService : PersistenceService) extends Actor {
  
 val log = Logging(context.system, PersistenceActor.this)
  
  def receive = {
    case msg: MasterSlaveProtocol => persistenceService.saveMasterMessage(msg)
    case msg: BasicMessage => persistenceService.saveBasicMessage(msg)
    case _ =>
  }
  

}