package org.nicta.wdy.hdm.message

/**
 * Created by Tiantian on 2014/12/19.
 */
trait CoordinatingMsg extends  Serializable

case class JoinMsg(sender:String, state:Int) extends CoordinatingMsg

case class LeaveMsg(sender:String) extends CoordinatingMsg
