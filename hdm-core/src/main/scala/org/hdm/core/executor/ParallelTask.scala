package org.hdm.core.executor

import java.util.concurrent.Callable

import org.hdm.core._
import org.hdm.core.functions.SerializableFunction
import org.hdm.core.model.{HDM, DDM, DataDependency}
import org.hdm.core.utils.Logging

import scala.reflect.ClassTag

/**
 * Created by tiantian on 23/11/15.
 */
abstract class ParallelTask [R : ClassTag] extends Serializable with Callable[Seq[DDM[_, R]]] with Logging {

  val appId: String

  val version: String

  val exeId:String

  val taskId: String

  val idx:Int

  def input:Seq[HDM[_]]

  val dep: DataDependency

  val keepPartition: Boolean

  val func:SerializableFunction[_, Arr[R]]

  val partitioner:Partitioner[R]

  val createTime: Long = System.currentTimeMillis()

  val appContext: AppContext

  var blockContext:BlockContext

  @transient
  protected var hdmContext: HDMContext = HDMContext.defaultHDMContext

  def setHDMContext(hdmContext: HDMContext): ParallelTask[R] ={
    this.hdmContext = hdmContext
    this
  }

  def setBlockContext(blockContext: BlockContext): ParallelTask[R] ={
    this.blockContext = blockContext
    this
  }

}
