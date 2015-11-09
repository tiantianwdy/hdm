package org.nicta.wdy.hdm.functions


import java.util

import org.apache.hadoop.util.MergeSort
import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.collections.BufUtils
import org.nicta.wdy.hdm.executor.{ShuffleBlockAggregator, Aggregator}
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.utils.SortingUtils

import scala.collection.mutable
import scala.collection.mutable.{HashMap, Buffer, ArrayBuffer}
import scala.reflect._
import scala.util.Sorting

//import scala.reflect.runtime.universe.WeakTypeTag


/**
 * Created by Tiantian on 2014/12/4.
 */


/**
 *
 * @tparam T input type
 * @tparam R return type
 */
abstract class ParallelFunction [T:ClassTag, R :ClassTag] extends SerializableFunction[Buf[T], Buf[R]] {


  val dependency:FuncDependency

  def andThen[U:ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
    val f = (seq:Buf[T]) => func(this.apply(seq))
    val combinedDep = if(this.dependency == FullDep || func.dependency == FullDep) FullDep else PartialDep
    if(this.dependency == PartialDep) {
      val a = (seq: Buf[T], res: Buf[U]) => {
        func.aggregate(this.apply(seq), res)
      }
      val post = (b:Buf[U]) => b
      new ParCombinedFunc[T,U,U](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post)
    } else {//if(this.dependency == FullDep)
      val a = (seq: Buf[T], res: Buf[R]) => {
        this.aggregate(seq, res)
      }
      val post = (b:Buf[R]) => func.apply(b)
      new ParCombinedFunc[T,R,U](dependency= combinedDep, parallel = f, preF = this.apply(_), aggregation = a, postF = post)
    }
  }

  def compose[I:ClassTag](func: ParallelFunction[I, T]): ParallelFunction[I, R] = {
    val f = (seq:Buf[I]) => this.apply(func.apply(seq))
    val combinedDep = if(func.dependency == FullDep || this.dependency == FullDep) FullDep else PartialDep
    if(func.dependency == PartialDep) {
      val a = (seq: Buf[I], res: Buf[R]) => {
        this.aggregate(func.apply(seq), res)
      }
      val post = (b:Buf[R]) => b
      new ParCombinedFunc[I,R,R](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post)
    } else {//if(func.dependency == FullDep)
    val a = (seq: Buf[I], res: Buf[T]) => {
        func.aggregate(seq, res)
      }
      val post = (b:Buf[T]) => this.apply(b)
      new ParCombinedFunc[I,T,R](dependency= combinedDep, parallel = f, preF = func.apply(_), aggregation = a, postF = post)
    }
  }

//  def getAggregator():Aggregator[Seq[T],Seq[R]]


  def aggregate(params:Buf[T], res:Buf[R]): Buf[R]

}


class ParMapFunc [T:ClassTag,R:ClassTag](val f: T=>R)  extends ParallelFunction[T,R] {

  val dependency = PartialDep


  override def andThen[U: ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
    if(func.isInstanceOf[ParMapFunc[_,_]]){
      val nf = func.asInstanceOf[ParMapFunc[R,U]]
      new ParMapFunc(f.andThen(nf.f))
    } else if(func.isInstanceOf[ParFindByFunc[_]]){
      val nf = func.asInstanceOf[ParFindByFunc[R]]
      val mapAll = (seq:Buf[T]) => {
        seq.filter(f.andThen(nf.f)).map(f)
      }
      new ParMapAllFunc(mapAll).asInstanceOf[ParallelFunction[T, U]]
    } else {
      super.andThen(func)
    }
  }

  override def apply(params: Buf[T]): Buf[R] = {
    params.map(f)
  }

  override def aggregate(params: Buf[T], res: Buf[R]): Buf[R] = {
//    res ++= params.map(f)
    BufUtils.combine(res, params.map(f))
  }

}

class ParMapAllFunc [T:ClassTag,R:ClassTag](f: Buf[T]=>Buf[R])  extends ParallelFunction[T,R] {


  val dependency = FullDep

  override def apply(params: Buf[T]): Buf[R] = {
    f(params)
  }

  override def aggregate(params: Buf[T], res: Buf[R]): Buf[R] = {
//    res ++= f(params)
    BufUtils.combine(res, f(params))
  }
}


class ParFindByFunc[T:ClassTag](val f: T=> Boolean)  extends ParallelFunction[T,T] {

  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Buf[T], res: Buf[T]): Buf[T] = {
//    res ++= this.apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Buf[T]): Buf[T] = {
    params.filter(f)
  }
}



class ParReduceFunc[T:ClassTag ,R >:T :ClassTag](val f: (R, R) => R)  extends ParallelFunction[T, R]{

  val dependency = FullDep

  override def apply(params: Buf[T]): Buf[R] = {
    Buf(params.reduce(f))
  }

  override def aggregate(params: Buf[T], res: Buf[R]): Buf[R] = {
    val elems = if(res.isEmpty) params.reduce(f)
     else params.fold(res.head)(f)
    Buf(elems)
  }
}

class ParFoldFunc[T:ClassTag, R:ClassTag](z:R)(f: (R, T) => R)  extends ParallelFunction[T, R] {

  val dependency = FullDep

  override def apply(params: Buf[T]): Buf[R] = {
    Buf(params.foldLeft(z)(f))
  }

  override def aggregate(params: Buf[T], res: Buf[R]): Buf[R] = {
    Buf(params.foldLeft(res.head)(f))
  }

}


class ParGroupByFunc[T: ClassTag, K: ClassTag](val f: T => K) extends ParallelFunction[T,(K,Buf[T])]  {

  val dependency = FullDep

  override def apply(params: Buf[T]): Buf[(K, Buf[T])] = {
    params.groupBy(f).toBuffer
  }


  override def aggregate(params: Buf[T], res: Buf[(K, Buf[T])]): Buf[(K, Buf[T])] = { // 40% faster than non-optimized one
//    val tempMap = res
    val tempMap = HashMap.empty[K,Buf[T]] ++= res
    params foreach {elem =>
      val k = f(elem)
      if(tempMap.contains(k)){
        val v = tempMap.apply(k)
//        tempMap.update(k, v += elem)
        tempMap.update(k, BufUtils.add(v,elem))
      } else {
        tempMap += k -> Buf(elem)
      }
    }

    tempMap.toBuffer
  }


  @deprecated("replcaced by follow up aggregator","0.0.1")
  def aggregateOld(params: Buf[T], res: Buf[(K, Buf[T])]): Buf[(K, Buf[T])] = {
    val tempMap = HashMap.empty[K,Buf[T]]
    res foreach { e =>
      tempMap += e._1 -> e._2
    }
    params foreach {elem =>
      val k = f(elem)
      if(tempMap.contains(k)){
        val v = tempMap.apply(k)
        //        tempMap.update(k, v += elem)
        tempMap.update(k, BufUtils.add(v,elem))
      } else {
        tempMap += k -> Buf(elem)
      }
    }
    tempMap.toBuffer
  }

//  def getAggregator(): Aggregator[Seq[T], Seq[(K, Seq[T])]] ={
//    val z = (e:T) => Buffer(f(e) -> Buffer(e))
//    val a = (e:T, v: Buffer[T]) => v += e
//    new ShuffleBlockAggregator(f = this.f(_), zero = z(_), aggr = a(_, _)  )
//  }
}

class ParReduceBy[T:ClassTag, K :ClassTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)] {


  val dependency = FullDep

  override def apply(params: Buf[T]): Buf[(K, T)] = {
//    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr))) // 30% slower than new implementation
    val tempMap = HashMap.empty[K,T]
    params foreach{ elem =>
      val k = fk(elem)
      if(tempMap.contains(k)){
        val v = tempMap.apply(k)
        tempMap.update(k, fr(v, elem))
      } else {
        tempMap += k -> elem
      }
    }
    tempMap.toBuffer
  }

  override def aggregate(params: Buf[T], res: Buf[(K, T)]): Buf[(K, T)] = {
    val tempMap = HashMap.empty[K,T] ++= res
    params foreach { elem =>
      val k = fk(elem)
      if (tempMap.contains(k)) {
        val v = tempMap.apply(k)
        tempMap.update(k, fr(v, elem))
      } else {
        tempMap += k -> elem
      }
    }
/*    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr))) foreach { tup =>
      if(tempMap.contains(tup._1)){
        val v = tempMap.apply(tup._1)
        tempMap.update(tup._1, fr(v, tup._2))
      } else {
        tempMap += tup
      }
    }*/
    tempMap.toBuffer
  }
}




class ParGroupByAggregation[T:ClassTag, K:ClassTag, R : ClassTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends ParallelFunction[T,(K,R)] {

  val dependency = FullDep

  override def apply(params: Buf[T]): Buf[(K, R)] = {
    params.groupBy(fk).mapValues(_.map(t).reduce(fr)).toBuffer
  }

  override def aggregate(params: Buf[T], res: Buf[(K, R)]): Buf[(K, R)] = {
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

  override def apply(params: Buf[T]): Buf[T] = {
    params
  }

  override def aggregate(params: Buf[T], res: Buf[T]): Buf[T] = {
//    res ++= params
    BufUtils.combine(res, params)
  }
}

class FlattenFunc[T: ClassTag] extends ParallelFunction[T,T] {

  val dependency = PartialDep

  override def apply(params: Buf[T]): Buf[T] = {
    params
  }

  override def aggregate(params: Buf[T], res: Buf[T]): Buf[T] = {
//    res ++= params
    BufUtils.combine(res, params)
  }

}

class NullFunc[T: ClassTag] extends ParallelFunction[T,T] {

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

  override def apply(params: Buf[T]): Buf[T] = {
    params
  }

  override def aggregate(params: Buf[T], res: Buf[T]): Buf[T] = {
//    res ++= params
    BufUtils.combine(res, params)
  }

}

class ParCombinedFunc [T:ClassTag,U:ClassTag,R:ClassTag](val dependency:FuncDependency, parallel: Buf[T]=>Buf[R],
                                                         val preF: Buf[T]=>Buf[U],
                                                         val aggregation:(Buf[T], Buf[U]) => Buf[U],
                                                         val postF: Buf[U] => Buf[R])  extends ParallelFunction[T,R]  {

  override def apply(params: Buf[T]): Buf[R] = {
    parallel(params)
  }

  override def aggregate(params: Buf[T], res: Buf[R]): Buf[R] = ???


  def partialAggregate(params: Buf[T], res: Buf[U]): Buf[U] = {
    aggregation(params,res)
  }

  val mediateType = classTag[U]
}


class SortFunc[T : ClassTag](val sortBeforeMerge:Boolean = false)(implicit ordering: Ordering[T]) extends ParallelFunction[T,T] {

  override val dependency: FuncDependency = FullDep


  override def apply(params: Buf[T]): Buf[T] = {
    if(classTag[T] == classTag[Int]){
      val array = params.toArray.asInstanceOf[Array[Int]]
      Sorting.quickSort(array)
      array.toBuffer.asInstanceOf[Buf[T]]
    } else {
      val array = params.toArray
//      Sorting.quickSort(array)
      Sorting.stableSort(array)
      array.toBuffer
    }
  }

  override def aggregate(params: Buf[T], sorted: Buf[T]): Buf[T] = {
    //todo change to support AnyVal
    classTag[T] match {
      case ClassTag.Int =>
//        println("sorting array of Int")
        val array = params.toArray.asInstanceOf[Array[Int]]
        //if params has not been sorted
        if(sortBeforeMerge)
          Sorting.quickSort(array)
        val resArray = sorted.toArray.asInstanceOf[Array[Int]]
        //merge sorted sequences
        SortingUtils.mergeSorted(resArray, array).toBuffer.asInstanceOf[Buf[T]]
      case other:Any =>
//        println("sorting array of Any")
        val array = params.toArray
        //if params has not been sorted
        if(sortBeforeMerge)
          Sorting.quickSort(array)
        val resArray = sorted.toArray
        //merge sorted sequences
        SortingUtils.mergeSorted(resArray, array).toBuffer
    }
  }

  def aggregateSorting(params: Buf[T], res: Buf[T]): Buf[T] = {
    //assume both res and params have not been unsorted
    //todo change to support AnyVal
    classTag[T] match {
      case ct:ClassTag[Int] =>
        val array = params.toArray.asInstanceOf[Array[Int]]
        //        Sorting.quickSort(array)
        val resArray = res.toArray.asInstanceOf[Array[Int]]
        val newRes = Array.concat(resArray, array)
        Sorting.quickSort(newRes)
        newRes.toBuffer.asInstanceOf[Buf[T]]
      case other:Any =>
        val array = params.toArray
        //        Sorting.quickSort(array)
        val resArray = res.toArray
        val newRes = Array.concat(resArray, array)
        Sorting.quickSort(newRes)
        newRes.toBuffer
    }
  }

}

object ParallelFunction {


}