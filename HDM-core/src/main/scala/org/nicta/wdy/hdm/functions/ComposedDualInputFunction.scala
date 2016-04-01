package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.{Buf, Arr}

import scala.reflect.ClassTag

/**
 * Created by tiantian on 31/03/16.
 */
class ComposedDualInputFunction[T: ClassTag, U: ClassTag, V: ClassTag, R: ClassTag](val dualFunc: DualInputFunction[T, U, V],
                                                                                    val pFunc: ParallelFunction[V, R])
  extends DualInputFunction[T, U, R] with Aggregator[(Arr[T], Arr[U]), Buf[R]] {

  @transient
  private var tempBuffer:Buf[V] = _

  override def apply(params: (Arr[T], Arr[U])): Arr[R] = {
    pFunc(dualFunc.apply(params))
  }

  override def aggregate(params: (Arr[T], Arr[U]), res: Buf[R]): Buf[R] = ???

  override def init(zero: Buf[R]): Unit = {
    tempBuffer = Buf.empty[V]
    dualFunc match {
      case agg: Aggregator[(Arr[T], Arr[U]), Buf[V]] => agg.init(tempBuffer)
      case other =>
    }
  }

  override def aggregate(params: (Arr[T], Arr[U])): Unit = {
    dualFunc match {
      case agg:Aggregator[(Arr[T], Arr[U]),Buf[V]] =>
        agg.aggregate(params)
      case other =>
        tempBuffer = dualFunc.aggregate(params , tempBuffer)
    }

  }

  override def result: Buf[R] = {
    dualFunc match {
      case agg: Aggregator[(Arr[T], Arr[U]), Buf[V]] =>
        pFunc(agg.result.toIterator).toBuffer
      case other =>
        pFunc(tempBuffer.toIterator).toBuffer
    }
  }

}
