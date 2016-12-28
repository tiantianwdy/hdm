package org.hdm.core.functions

import java.util.concurrent.atomic.AtomicReference

import org.hdm.core._
import org.hdm.core.executor.TaskContext
import org.hdm.core.model.{PartialDep, FullDep, FuncDependency}

import scala.collection.mutable.Buffer
import scala.reflect._

/**
 * Created by tiantian on 1/05/16.
 */
class ParCombinedFunc [T:ClassTag,U:ClassTag,R:ClassTag](val dependency:FuncDependency, parallel: Arr[T]=>Arr[R],
                                                         val preF: Arr[T]=>Arr[U],
                                                         val aggregation:(Arr[T], Buf[U]) => Buf[U],
                                                         val postF: Arr[U] => Arr[R],
                                                         var parentFunc:ParallelFunction[T,_],
                                                         var curFUnc:ParallelFunction[_,R])
                                                          extends ParallelFunction[T,R]
                                                          with Aggregator[Arr[T], Buf[R]]  {
  val mediateType = classTag[U]

  @transient
  private var midBuffer: Buf[U] = Buf.empty[U]

  override def setTaskContext(context: TaskContext): Unit = {
    parentFunc.setTaskContext(context)
    curFUnc.setTaskContext(context)
    if(runTimeContext == null) runTimeContext = new  ThreadLocal[TaskContext]()
    runTimeContext.set(context)
  }

  override def apply(params: Arr[T]): Arr[R] = {
    parallel(params)
  }

  override def aggregate(params: Arr[T], res: Buffer[R]): Buffer[R] = ???


  def partialAggregate(params: Arr[T], res: Buffer[U]): Buffer[U] = {
    aggregation(params,res)
  }


  override def init(zero: Buf[R]): Unit = {
    midBuffer = Buf.empty[U]
  }

  override def result: Buf[R] = {
    postF(midBuffer.toIterator).toBuffer
  }

  override def aggregate(params: Arr[T]): Unit = {

    midBuffer = aggregation(params, midBuffer)

  }
}


object ParCombinedFunc{

  /**
   * construct a combined function from two parallel functions
   * @param first
   * @param second
   * @tparam T
   * @tparam U
   * @tparam R
   * @return
   */
  def apply[T:ClassTag,U:ClassTag,R:ClassTag](first:ParallelFunction[T,U], second:ParallelFunction[U, R]): ParallelFunction[T, R] ={
      val f = (seq:Arr[T]) => second(first.apply(seq))
      val combinedDep = if(first.dependency == FullDep || second.dependency == FullDep) FullDep else PartialDep
      if(first.dependency == PartialDep) {
        val a = (seq: Arr[T], res: Buf[R]) => {
          second.aggregate(first.apply(seq), res)
        }
        val post = (b:Arr[R]) => b
        new ParCombinedFunc[T, R, R](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post, parentFunc = first, curFUnc = second)
      } else {//if(this.dependency == FullDep)
      val a = (seq: Arr[T], res: Buf[U]) => {
          first.aggregate(seq, res)
        }
        val post = (b:Arr[U]) => second.apply(b)
        new ParCombinedFunc[T, U, R](dependency= combinedDep, parallel = f, preF = first.apply(_), aggregation = a, postF = post, parentFunc = first, curFUnc = second)
      }
  }
}

