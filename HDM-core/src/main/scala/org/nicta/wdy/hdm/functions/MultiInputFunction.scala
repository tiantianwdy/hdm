package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm._

import scala.collection.mutable.{Buffer, HashMap}

/**
 * Created by tiantian on 23/11/15.
 */
trait TwoInputFunction[T, U, R] extends SerializableFunction[(Arr[T], Arr[U]), Arr[R]] with Aggregatable[(Arr[T], Arr[U]), Buf[R]] {

}


trait ThreeInputFunction[T, U, I, R] extends SerializableFunction[(Arr[T], Arr[U], Arr[I]), Arr[R]] with Aggregatable[(Arr[T], Arr[U], Arr[I]), Buf[R]] {

}


class CoGroupFunc[T, U, K](f1:T => K, f2: U => K ) extends TwoInputFunction[T, U, (K, (Iterable[T], Iterable[U]))] {

  override def apply(params: (Arr[T], Arr[U])): Arr[(K, (Iterable[T], Iterable[U]))] = {
    val res = HashMap.empty[K, (Iterable[T], Iterable[U])]
    params._1.foreach{ elem =>
      val key = f1(elem)
      val buffer = res.getOrElseUpdate(key, (Buf.empty[T], Buf.empty[U]))
      buffer._1.asInstanceOf[Buf[T]] += elem
    }
    params._2.foreach{
      elem =>
        val key = f2(elem)
        val buffer = res.getOrElseUpdate(key, (Buf.empty[T], Buf.empty[U]))
        buffer._2.asInstanceOf[Buf[U]] += elem
    }
    res.toIterator
  }

  override def aggregate(params: (Arr[T], Arr[U]), res: Buffer[(K, (Iterable[T], Iterable[U]))]): Buffer[(K, (Iterable[T], Iterable[U]))] = {
    val tempMap = HashMap.empty[K,(Iterable[T], Iterable[U])] ++= res
    params._1.foreach{ elem =>
      val key = f1(elem)
      val buffer = tempMap.getOrElseUpdate(key, (Buf.empty[T], Buf.empty[U]))
      buffer._1.asInstanceOf[Buf[T]] += elem
    }
    params._2.foreach{
      elem =>
        val key = f2(elem)
        val buffer = tempMap.getOrElseUpdate(key, (Buf.empty[T], Buf.empty[U]))
        buffer._2.asInstanceOf[Buf[U]] += elem
    }
    tempMap.toBuffer
  }
}