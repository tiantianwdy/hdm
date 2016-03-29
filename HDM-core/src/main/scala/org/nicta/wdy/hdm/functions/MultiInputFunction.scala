package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm._

import scala.collection.mutable.{Buffer, HashMap}

/**
 * Created by tiantian on 23/11/15.
 */
trait DualInputFunction[T, U, R] extends SerializableFunction[(Arr[T], Arr[U]), Arr[R]] with Aggregatable[(Arr[T], Arr[U]), Buf[R]] {

}


trait ThreeInputFunction[T, U, I, R] extends SerializableFunction[(Arr[T], Arr[U], Arr[I]), Arr[R]] with Aggregatable[(Arr[T], Arr[U], Arr[I]), Buf[R]] {

}


class CoGroupFunc[T, U, K](f1:T => K, f2: U => K ) extends DualInputFunction[T, U, (K, (Iterable[T], Iterable[U]))]
                                                   with Aggregator[(Arr[T],Arr[U]), Buf[(K, (Iterable[T], Iterable[U]))]]{
  @transient
  private var tempBuffer = HashMap.empty[K,(Iterable[T], Iterable[U])]

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

  override def init(zero: Buf[(K, (Iterable[T], Iterable[U]))]): Unit = {
    tempBuffer ++= zero
  }

  override def aggregate(params: (Arr[T], Arr[U])): Unit = {
    params._1.foreach{ elem =>
      val key = f1(elem)
      val buffer = tempBuffer.getOrElseUpdate(key, (Buf.empty[T], Buf.empty[U]))
      buffer._1.asInstanceOf[Buf[T]] += elem
    }
    params._2.foreach{
      elem =>
        val key = f2(elem)
        val buffer = tempBuffer.getOrElseUpdate(key, (Buf.empty[T], Buf.empty[U]))
        buffer._2.asInstanceOf[Buf[U]] += elem
    }
  }

  override def result: Buf[(K, (Iterable[T], Iterable[U]))] = {
    tempBuffer.toBuffer
  }


}

class JoinByFunc[T, U, K](f1:T => K, f2: U => K ) extends DualInputFunction[T, U, (Iterable[T], Iterable[U])]
                                                  with Aggregator[(Arr[T],Arr[U]), Buf[(Iterable[T], Iterable[U])]]{

  private var res: HashMap[K,(Iterable[T], Iterable[U])] = _

  override def apply(params: (Arr[T], Arr[U])): Arr[(Iterable[T], Iterable[U])] = {
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
    res.map(_._2).toIterator
  }


  override def init(zero: Buf[(Iterable[T], Iterable[U])]): Unit = {
    res = HashMap.empty[K, (Iterable[T], Iterable[U])]
  }

  override def aggregate(params: (Arr[T], Arr[U])): Unit = {
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
  }

  override def result: Buf[(Iterable[T], Iterable[U])] = {
    res.map(_._2).toBuffer
  }


  override def aggregate(params: (Arr[T], Arr[U]), res: Buffer[(Iterable[T], Iterable[U])]): Buffer[(Iterable[T], Iterable[U])] = ???

}