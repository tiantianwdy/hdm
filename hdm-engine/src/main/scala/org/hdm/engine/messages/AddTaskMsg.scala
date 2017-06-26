package org.hdm.engine.messages

import org.hdm.core.executor.ParallelTask
import org.hdm.core.message.SchedulingMsg

import scala.reflect.ClassTag

/**
  * Created by tiantian on 26/06/17.
  */
case class AddTaskMsg[R :ClassTag](task:ParallelTask[R]) extends SchedulingMsg
