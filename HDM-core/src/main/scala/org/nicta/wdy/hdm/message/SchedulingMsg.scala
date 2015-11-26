package org.nicta.wdy.hdm.message

import org.nicta.wdy.hdm.executor.{ParallelTask, Task}
import org.nicta.wdy.hdm.model.{DDM, HDM}
import scala.reflect.ClassTag
import scala.language.existentials

/**
 * Created by Tiantian on 2014/12/18.
 */
trait SchedulingMsg extends Serializable

case class AddTaskMsg[R :ClassTag](task:ParallelTask[R]) extends SchedulingMsg

case class TaskCompleteMsg(appId:String, taskId:String, func:String, result:Seq[DDM[_,_]]) extends SchedulingMsg

case class AddHDMsMsg(appId:String, hdms:Seq[HDM[_,_]], resultReceiver:String) extends SchedulingMsg

case class SubmitJobMsg(appId:String, hdm:HDM[_,_], resultReceiver:String, parallelism:Int) extends SchedulingMsg

case class JobCompleteMsg(appId:String, state:Int, result:Any) extends SchedulingMsg

case class StopTaskMsg(appId:String, taskId:String) extends SchedulingMsg
