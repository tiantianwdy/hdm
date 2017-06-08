package org.hdm.core.message

import java.util.UUID

/**
 * Created by Tiantian on 2014/12/19.
 */
trait CoordinatingMsg extends  Serializable

case class JoinMsg(sender:String, state:Int) extends CoordinatingMsg

case class LeaveMsg(senders:Seq[String]) extends CoordinatingMsg

case class CollaborateMsg(sender:String, state:Int) extends CoordinatingMsg

case class AskCollaborateMsg(sibling:String) extends CoordinatingMsg

case class ResSync(msgId:String = UUID.randomUUID().toString, resId:String, change:Int) extends CoordinatingMsg

case class ResSyncResp(msgId:String, state:Int) extends CoordinatingMsg

case class MigrationMsg(worker:String, toMaster:String) extends CoordinatingMsg
