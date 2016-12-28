package org.hdm.core.message

import org.hdm.core.executor.{ParallelTask, Task}
import org.hdm.core.model.{DDM, ParHDM}
import org.hdm.core.serializer.SerializableByteBuffer
import scala.reflect.ClassTag
import scala.language.existentials

/**
 * Created by Tiantian on 2014/12/18.
 */
trait SchedulingMsg extends Serializable

case class AddTaskMsg[R :ClassTag](task:ParallelTask[R]) extends SchedulingMsg

case class TaskCompleteMsg(appId:String, taskId:String, func:String, result:Seq[DDM[_,_]]) extends SchedulingMsg

case class AddHDMsMsg(appId:String, hdms:Seq[ParHDM[_,_]], resultReceiver:String) extends SchedulingMsg

case class SubmitJobMsg(appId:String, hdm:ParHDM[_,_], resultReceiver:String, parallelism:Int) extends SchedulingMsg

case class JobCompleteMsg(appId:String, state:Int, result:Any) extends SchedulingMsg

case class StopTaskMsg(appId:String, taskId:String) extends SchedulingMsg

case class SerializedJobMsg(appName:String, version:String, serJob:Array[Byte], resultReceiver:String, source:String, parallelism:Int) extends SchedulingMsg

case class SerializedTaskMsg(appName:String, version:String, taskID:String, serTask:Array[Byte]) extends SchedulingMsg

case class RegisterPromiseMsg(appName:String, version:String, lastTaskID:String, triggerPath:String) extends SchedulingMsg