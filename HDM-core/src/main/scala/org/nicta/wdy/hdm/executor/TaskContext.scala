package org.nicta.wdy.hdm.executor

/**
 * Created by tiantian on 30/04/16.
 */
case class TaskContext(taskIdx:Int) extends Serializable


object TaskContext {

  def apply(parallelTask: ParallelTask[_]):TaskContext = {
    TaskContext(parallelTask.idx)
  }
}