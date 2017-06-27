package org.hdm.core.coordinator

import akka.remote.RemotingLifecycleEvent
import org.hdm.akka.actors.worker.WorkActor
import org.hdm.core.message.{CoordinatingMsg, DependencyMsg, SchedulingMsg}
import org.hdm.core.messages.QueryMsg

/**
 * Created by tiantian on 9/05/16.
 */

trait MsgReceiver {

  this: WorkActor =>
}


trait QueryReceiver extends MsgReceiver{

  this: WorkActor =>

  def processQueries: PartialFunction[QueryMsg, Unit]

}

trait CoordinationReceiver extends MsgReceiver{

  this: WorkActor =>

  def processCoordinationMsg: PartialFunction[CoordinatingMsg, Unit]
}

trait DepMsgReceiver extends MsgReceiver{

  this: WorkActor =>

  def processDepMsg: PartialFunction[DependencyMsg, Unit]

}

trait SchedulingMsgReceiver extends MsgReceiver{

  this: WorkActor =>

  def processScheduleMsg: PartialFunction[SchedulingMsg, Unit]

}


trait RemotingEventManager extends MsgReceiver {

  this: WorkActor =>

  def processRemotingEvents: PartialFunction[RemotingLifecycleEvent, Unit]

}
