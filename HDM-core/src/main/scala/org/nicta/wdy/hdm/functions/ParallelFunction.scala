package org.nicta.wdy.hdm.functions


import org.nicta.wdy.hdm.model.{FullDep, PartialDep, FuncDependency, DataDependency}

import scala.collection.mutable
import scala.collection.mutable.{HashMap, Buffer, ArrayBuffer}
import scala.reflect._
//import scala.reflect.runtime.universe.WeakTypeTag


/**
 * Created by Tiantian on 2014/12/4.
 */


/**
 *
 * @tparam T input type
 * @tparam R return type
 */
abstract class ParallelFunction [T:ClassTag, R :ClassTag] extends SerializableFunction[Seq[T], Seq[R]] {


  val dependency:FuncDependency

  def andThen[U:ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
    val f = (seq:Seq[T]) => func(this.apply(seq))
    val combinedDep = if(this.dependency == FullDep || func.dependency == FullDep) FullDep else PartialDep
    if(this.dependency == PartialDep) {
      val a = (seq: Seq[T], res: mutable.Buffer[U]) => {
        func.aggregate(this.apply(seq), res)
      }
      val post = (b:Buffer[U]) => b
      new ParCombinedFunc[T,U,U](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post)
    } else {//if(this.dependency == FullDep)
      val a = (seq: Seq[T], res: mutable.Buffer[R]) => {
        this.aggregate(seq, res)
      }
      val post = (b:Buffer[R]) => func.apply(b).toBuffer[U]
      new ParCombinedFunc[T,R,U](dependency= combinedDep, parallel = f, preF = this.apply(_), aggregation = a, postF = post)
    }
  }

  def compose[I:ClassTag](func: ParallelFunction[I, T]): ParallelFunction[I, R] = {
    val f = (seq:Seq[I]) => this.apply(func.apply(seq))
    val combinedDep = if(func.dependency == FullDep || this.dependency == FullDep) FullDep else PartialDep
    if(func.dependency == PartialDep) {
      val a = (seq: Seq[I], res: mutable.Buffer[R]) => {
        this.aggregate(func.apply(seq), res)
      }
      val post = (b:Buffer[R]) => b
      new ParCombinedFunc[I,R,R](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post)
    } else {//if(func.dependency == FullDep)
    val a = (seq: Seq[I], res: mutable.Buffer[T]) => {
        func.aggregate(seq, res)
      }
      val post = (b:Buffer[T]) => this.apply(b).toBuffer
      new ParCombinedFunc[I,T,R](dependency= combinedDep, parallel = f, preF = func.apply(_), aggregation = a, postF = post)
    }
  }


  def aggregate(params:Seq[T], res:Buffer[R]): Buffer[R]

}


class ParMapFunc [T:ClassTag,R:ClassTag](f: T=>R)  extends ParallelFunction[T,R] {

  val dependency = PartialDep

  override def apply(params: Seq[T]): Seq[R] = {
    params.map(f)
  }

  override def aggregate(params: Seq[T], res: Buffer[R]): Buffer[R] = {
    res ++= params.map(f)
  }
}

class ParMapAllFunc [T:ClassTag,R:ClassTag](f: Seq[T]=>Seq[R])  extends ParallelFunction[T,R] {

  val dependency = FullDep

  override def apply(params: Seq[T]): Seq[R] = {
    f(params)
  }

  override def aggregate(params: Seq[T], res: Buffer[R]): Buffer[R] = {
    res ++= f(params)
  }
}

class ParCombinedFunc [T:ClassTag,U:ClassTag,R:ClassTag](val dependency:FuncDependency, parallel: Seq[T]=>Seq[R],
                                                         val preF: Seq[T]=>Seq[U],
                                                         val aggregation:(Seq[T], Buffer[U]) => Buffer[U],
                                                         val postF: Buffer[U] => Buffer[R])  extends ParallelFunction[T,R] {

  override def apply(params: Seq[T]): Seq[R] = {
    parallel(params)
  }

  override def aggregate(params: Seq[T], res: Buffer[R]): Buffer[R] = ???


  def partialAggregate(params: Seq[T], res: Buffer[U]): Buffer[U] = {
    aggregation(params,res)
  }

  val mediateType = classTag[U]
}


class ParReduceFunc[T:ClassTag ,R >:T :ClassTag](f: (R, R) => R)  extends ParallelFunction[T, R] {

  val dependency = PartialDep

  override def apply(params: Seq[T]): Seq[R] = {
    Seq(params.reduce(f))
  }

  override def aggregate(params: Seq[T], res: Buffer[R]): Buffer[R] = {
    val elems = if(res.isEmpty) params.reduce(f)
     else params.fold(res.head)(f)
    ArrayBuffer(elems)
  }
}

class ParFoldFunc[T:ClassTag, R:ClassTag](z:R)(f: (R, T) => R)  extends ParallelFunction[T, R] {

  val dependency = FullDep

  override def apply(params: Seq[T]): Seq[R] = {
    Seq(params.foldLeft(z)(f))
  }

  override def aggregate(params: Seq[T], res: Buffer[R]): Buffer[R] = {
    ArrayBuffer(params.foldLeft(res.head)(f))
  }

}


class ParGroupByFunc[T: ClassTag, K: ClassTag](f: T => K) extends ParallelFunction[T,(K,Seq[T])] {

  val dependency = FullDep

  override def apply(params: Seq[T]): Seq[(K, Seq[T])] = {
    params.groupBy(f).toSeq
  }

  override def aggregate(params: Seq[T], res: Buffer[(K, Seq[T])]): Buffer[(K, Seq[T])] = {
    val temp = HashMap.empty[K,Seq[T]] ++= res //todo change to use AppendOnlyMap
    params.groupBy(f) foreach{ tup =>
      if(temp.contains(tup._1)){
        val v = temp.apply(tup._1)
        temp.update(tup._1, v ++ tup._2)
      } else {
        temp += tup
      }
    }
   temp.toBuffer
  }
}

class ParReduceByKey[T:ClassTag, K :ClassTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)]{


  val dependency = FullDep

  override def apply(params: Seq[T]): Seq[(K, T)] = {
    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr)))
  }

  override def aggregate(params: Seq[T], res: Buffer[(K, T)]): Buffer[(K, T)] = {
    val tempMap = HashMap.empty[K,T] ++= res
    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr))) foreach { tup =>
      if(tempMap.contains(tup._1)){
        val v = tempMap.apply(tup._1)
        tempMap.update(tup._1, fr(v, tup._2))
      } else {
        tempMap += tup
      }
    }
    tempMap.toBuffer
  }
}

class ParMergeByKey[T:ClassTag, K :ClassTag](fr: (T, T) => T) extends ParallelFunction[(K,T),(K,T)]{


  val dependency = FullDep

  override def apply(params: Seq[(K, T)]): Seq[(K, T)] = {
    params.groupBy(_._1).toSeq.map(d => (d._1, d._2.map(_._2).reduce(fr)))
  }

  override def aggregate(params: Seq[(K, T)], res: Buffer[(K, T)]): Buffer[(K, T)] = {
    val tempMap = HashMap.empty[K,T] ++= res
    params.groupBy(_._1).toSeq.map(d => (d._1, d._2.map(_._2).reduce(fr))) foreach { tup =>
      if(tempMap.contains(tup._1)){
        val v = tempMap.apply(tup._1)
        tempMap.update(tup._1, fr(v, tup._2))
      } else {
        tempMap += tup
      }
    }
    tempMap.toBuffer
  }
}


class ParGroupFoldByKey[T:ClassTag, K:ClassTag, R : ClassTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends ParallelFunction[T,(K,R)]{

  val dependency = FullDep

  override def apply(params: Seq[T]): Seq[(K, R)] = {
    params.groupBy(fk).mapValues(_.map(t).reduce(fr)).toSeq
  }

  override def aggregate(params: Seq[T], res: mutable.Buffer[(K, R)]): mutable.Buffer[(K, R)] = {
    val mapRes = params.groupBy(fk).mapValues(_.map(t).reduce(fr))
    val tempMap = HashMap.empty[K,R] ++= res
    mapRes.toSeq foreach { tup =>
      if(tempMap.contains(tup._1)){
        val v = tempMap.apply(tup._1)
        tempMap.update(tup._1, fr(v, tup._2))
      } else {
        tempMap += tup
      }
    }
    tempMap.toBuffer
  }
}

class ParUnionFunc[T: ClassTag]()  extends ParallelFunction[T,T] {

  val dependency = PartialDep

  override def apply(params: Seq[T]): Seq[T] = {
    params
  }

  override def aggregate(params: Seq[T], res: Buffer[T]): Buffer[T] = {
    res ++= params
  }
}

class FlattenFunc[T: ClassTag] extends ParallelFunction[T,T]{

  val dependency = PartialDep

  override def apply(params: Seq[T]): Seq[T] = {
    params
  }

  override def aggregate(params: Seq[T], res: Buffer[T]): Buffer[T] = {
    res ++= params
  }

}

class NullFunc[T: ClassTag] extends ParallelFunction[T,T]{

  val dependency = PartialDep
  /**
   * any function combined with null function would get itself.
   * @param func
   * @tparam U
   * @return
   */
  override def andThen[U: ClassTag](func: ParallelFunction[T, U]): ParallelFunction[T, U] = {
    func
  }


  override def compose[I: ClassTag](func: ParallelFunction[I, T]): ParallelFunction[I, T] = {
    func
  }

  override def apply(params: Seq[T]): Seq[T] = {
    params
  }

  override def aggregate(params: Seq[T], res: Buffer[T]): Buffer[T] = {
    res ++= params
  }

}

object ParallelFunction {


}