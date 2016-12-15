package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.{Buf, Arr}

import scala.reflect.ClassTag

/**
 * Created by tiantian on 31/03/16.
 */

/**
 * a composed function object used for combining dual input functions (functions with dual input types) with a subsequent parallel function (functions with only one input type)
 * @param dualFunc
 * @param pFunc
 * @tparam T
 * @tparam U
 * @tparam V
 * @tparam R
 */
class ComposedDualInputFunction[T: ClassTag, U: ClassTag, V: ClassTag, R: ClassTag](val dualFunc: DualInputFunction[T, U, V],
                                                                                    val pFunc: ParallelFunction[V, R])
  extends DualInputFunction[T, U, R] with Aggregator[(Arr[T], Arr[U]), Buf[R]] {

  @transient
  private val tempBuffer:ThreadLocal[Buf[V]] = new ThreadLocal[Buf[V]]

  override def apply(params: (Arr[T], Arr[U])): Arr[R] = {
    pFunc(dualFunc.apply(params))
  }

  override def aggregate(params: (Arr[T], Arr[U]), res: Buf[R]): Buf[R] = ???

  override def init(zero: Buf[R]): Unit = {
    tempBuffer.set(Buf.empty[V])
    dualFunc match {
      case agg: Aggregator[(Arr[T], Arr[U]), Buf[V]] => agg.init(tempBuffer.get())
      case other =>
    }
  }

  override def aggregate(params: (Arr[T], Arr[U])): Unit = {
    dualFunc match {
      case agg:Aggregator[(Arr[T], Arr[U]),Buf[V]] =>
        agg.aggregate(params)
      case other =>
        tempBuffer.set(dualFunc.aggregate(params , tempBuffer.get()))
    }

  }

  override def result: Buf[R] = {
    dualFunc match {
      case agg: Aggregator[(Arr[T], Arr[U]), Buf[V]] =>
        pFunc(agg.result.toIterator).toBuffer
      case other =>
        pFunc(tempBuffer.get().toIterator).toBuffer
    }
  }

}
