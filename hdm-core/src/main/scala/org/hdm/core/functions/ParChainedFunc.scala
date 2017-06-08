package org.hdm.core.functions

import org.hdm.core._
import org.hdm.core.executor.TaskContext
import org.hdm.core.model.{FullDep, PartialDep}

import scala.collection.mutable
import scala.reflect._

/**
  * Created by tiantian on 30/04/16.
  */
class ParChainedFunc[T: ClassTag, U: ClassTag, R: ClassTag](val preFunc: ParallelFunction[T, U], //should always been partial dependency or [[NullFunc]]
                                                            val postFunc: ParallelFunction[U, R] //can be both partial and full dependency
                                                           ) extends ParallelFunction[T, R] with Aggregator[Arr[T], Buf[R]] {

  assert(preFunc != null && postFunc != null)

  val mediateType = classTag[U]

  val dependency = if (postFunc.dependency == FullDep || preFunc.dependency == FullDep) FullDep else PartialDep

  @transient
  private var preBuffer: Array[T] = Array.empty[T]
  @transient
  private var midBuffer: Array[U] = Array.empty[U]
  @transient
  private var postBuffer: Array[R] = Array.empty[R]


  override def andThen[V: ClassTag](func: ParallelFunction[R, V]): ParallelFunction[T, V] = {
    if (this.dependency == PartialDep) {
      new ParChainedFunc(this, func)
    } else {
      val nPost = new ParChainedFunc(postFunc, func)
      new ParChainedFunc(preFunc, nPost)
    }
  }

  override def setTaskContext(context: TaskContext): Unit = {
    preFunc.setTaskContext(context)
    postFunc.setTaskContext(context)
    if (runTimeContext == null) runTimeContext = new ThreadLocal[TaskContext]()
    runTimeContext.set(context)
  }

  override def apply(params: Arr[T]): Arr[R] = {
    postFunc.apply(preFunc.apply(params))
  }

  override def aggregate(params: Arr[T], res: mutable.Buffer[R]): mutable.Buffer[R] = {
    if (this.dependency == PartialDep) {
      res ++= this.apply(params)
      res
    } else {
      postFunc.apply(preFunc.aggregate(params, mutable.Buffer.empty[U]).toIterator).toBuffer
    }
  }

  override def init(zero: Buf[R]): Unit = {
    if (this.dependency == PartialDep) {
      postBuffer = zero.toArray
    } else if (preFunc.dependency == PartialDep) {
      midBuffer = Array.empty[U]
    } else {
      preBuffer = Array.empty[T]
    }
  }

  override def result: Buf[R] = {
    if (this.dependency == PartialDep) {
      postBuffer.toBuffer
    } else if (preFunc.dependency == PartialDep) {
      postFunc.apply(midBuffer.toIterator).toBuffer
    } else {
      this.apply(preBuffer.toIterator).toBuffer
    }
  }

  override def aggregate(params: Arr[T]): Unit = {
    if (this.dependency == PartialDep) {
      postBuffer ++= this.apply(params)
    } else if (preFunc.dependency == PartialDep) {
      midBuffer ++= preFunc.apply(params)
    } else {
      preBuffer ++= params
    }
  }
}
