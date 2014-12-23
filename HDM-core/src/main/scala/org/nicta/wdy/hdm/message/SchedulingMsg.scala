package org.nicta.wdy.hdm.message

import org.nicta.wdy.hdm.executor.Task
import org.nicta.wdy.hdm.model.HDM

/**
 * Created by Tiantian on 2014/12/18.
 */
trait SchedulingMsg extends Serializable

case class AddTaskMsg(task:Task[_, _]) extends SchedulingMsg

case class TaskCompleteMsg(appId:String, taskId:String, state:Int) extends SchedulingMsg

case class AddJobMsg(appId:String, hdm:Seq[HDM[_,_]], resultReceiver:String)  extends SchedulingMsg

case class JobCompleteMsg(appId:String, state:Int, result:Any) extends SchedulingMsg

case class StopTaskMsg(appId:String, taskId:String) extends SchedulingMsg
