package com.baidu.bpit.akka.actors.worker

import akka.actor.Actor
import com.baidu.bpit.akka.messages.BasicMessage
import com.baidu.bpit.akka.messages.HeartbeatMsg
import akka.event.slf4j.Logger
import akka.event.Logging
import com.baidu.bpit.akka.persistence.PersistenceService
import com.baidu.bpit.akka.messages.MasterSlaveProtocol


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