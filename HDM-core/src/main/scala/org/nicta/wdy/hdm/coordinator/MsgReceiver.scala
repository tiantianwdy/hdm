package org.nicta.wdy.hdm.coordinator

import org.hdm.akka.actors.worker.WorkActor
import org.nicta.wdy.hdm.message.{SchedulingMsg, DependencyMsg, CoordinatingMsg, QueryMsg}

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

trait ClusterMsgReceiver extends MsgReceiver{

  this: WorkActor =>

  def processClusterMsg: PartialFunction[CoordinatingMsg, Unit]
}

trait DepMsgReceiver extends MsgReceiver{

  this: WorkActor =>

  def processDepMsg: PartialFunction[DependencyMsg, Unit]

}

trait SchedulingMsgReceiver extends MsgReceiver{

  this: WorkActor =>

  def processScheduleMsg: PartialFunction[SchedulingMsg, Unit]

}
