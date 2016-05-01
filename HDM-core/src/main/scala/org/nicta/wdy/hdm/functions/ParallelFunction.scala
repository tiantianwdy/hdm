package org.nicta.wdy.hdm.functions


import java.util.concurrent.atomic.AtomicReference

import org.nicta.wdy.hdm.collections.BufUtils
import org.nicta.wdy.hdm.executor.TaskContext
import org.nicta.wdy.hdm.model._
import org.nicta.wdy.hdm.utils.SortingUtils
import org.nicta.wdy.hdm.{Arr, Buf}

import scala.collection.mutable
import scala.collection.mutable.{Buffer, HashMap}
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
abstract class ParallelFunction [T:ClassTag, R :ClassTag] extends SerializableFunction[Arr[T], Arr[R]] with Aggregatable[Arr[T], Buffer[R]]{


  val dependency:FuncDependency

  @transient
  protected var runTimeContext:AtomicReference[TaskContext] = new  AtomicReference[TaskContext]()

  def setTaskContext(context:TaskContext) = {
    if(runTimeContext == null) runTimeContext = new  AtomicReference[TaskContext]()
    runTimeContext.set(context)
  }

  def removeTaskContext() = runTimeContext.set(null)

  def getTaskContext() = runTimeContext.get()

  def has(feature:FunctionFeature):Boolean = ParallelFunction.hasFeature(this, feature)

  def none(feature:FunctionFeature):Boolean = !has(feature)

  def andThen[U:ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
    ParCombinedFunc(this, func)
  }

  def compose[I:ClassTag](func: ParallelFunction[I, T]): ParallelFunction[I, R] = {
    ParCombinedFunc(func, this)
  }

//  def andThen[U:ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
//    val f = (seq:Arr[T]) => func(this.apply(seq))
//    val combinedDep = if(this.dependency == FullDep || func.dependency == FullDep) FullDep else PartialDep
//    if(this.dependency == PartialDep) {
//      val a = (seq: Arr[T], res: Buf[U]) => {
//        func.aggregate(this.apply(seq), res)
//      }
//      val post = (b:Arr[U]) => b
//      new ParCombinedFunc[T,U,U](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post, parentFunc = this, curFUnc = func)
//    } else {//if(this.dependency == FullDep)
//      val a = (seq: Arr[T], res: Buf[R]) => {
//        this.aggregate(seq, res)
//      }
//      val post = (b:Arr[R]) => func.apply(b)
//      new ParCombinedFunc[T,R,U](dependency= combinedDep, parallel = f, preF = this.apply(_), aggregation = a, postF = post, parentFunc = this, curFUnc = func)
//    }
//  }

//  def compose[I:ClassTag](func: ParallelFunction[I, T]): ParallelFunction[I, R] = {
//    val f = (seq:Arr[I]) => this.apply(func.apply(seq))
//    val combinedDep = if(func.dependency == FullDep || this.dependency == FullDep) FullDep else PartialDep
//    if(func.dependency == PartialDep) {
//      val a = (seq: Arr[I], res: Buf[R]) => {
//        this.aggregate(func.apply(seq), res)
//      }
//      val post = (b:Arr[R]) => b
//      new ParCombinedFunc[I,R,R](dependency= combinedDep, parallel = f, preF = f, aggregation = a, postF = post, parentFunc = func, curFUnc = this)
//    } else {//if(func.dependency == FullDep)
//    val a = (seq: Arr[I], res: Buf[T]) => {
//        func.aggregate(seq, res)
//      }
//      val post = (b:Arr[T]) => this.apply(b)
//      new ParCombinedFunc[I,T,R](dependency= combinedDep, parallel = f, preF = func.apply(_), aggregation = a, postF = post, parentFunc = func, curFUnc = this)
//    }
//  }

//  def getAggregator():Aggregator[Seq[T],Seq[R]]


//  def aggregate(params:Arr[T], res:mutable.Buffer[R]): mutable.Buffer[R]

}


class ParMapFunc [T:ClassTag,R:ClassTag](val f: T=>R)  extends ParallelFunction[T,R] {

  val dependency = PartialDep


  override def andThen[U: ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
    if(func.isInstanceOf[ParMapFunc[_,_]]){
      val nf = func.asInstanceOf[ParMapFunc[R,U]]
      new ParMapFunc(f.andThen(nf.f))
//      super.andThen(func)
    } else if(func.isInstanceOf[ParFindByFunc[_]]){
      val nf = func.asInstanceOf[ParFindByFunc[R]]
      val mapAll = (seq:Arr[T]) => {
        seq.filter(f.andThen(nf.f)).map(f)
      }
      new ParMapAllFunc(mapAll).asInstanceOf[ParallelFunction[T, U]]
    } else {
      super.andThen(func)
    }
  }

  override def apply(params: Arr[T]): Arr[R] = {
    params.map(f)
  }

  override def aggregate(params: Arr[T], res: Buffer[R]): Buffer[R] = {
//    res ++= params.map(f)
    BufUtils.combine(res, params.map(f))
  }

}

class ParMapAllFunc [T:ClassTag,R:ClassTag](val f: Arr[T]=>Arr[R])  extends ParallelFunction[T,R] {


  val dependency = FullDep


  override def apply(params: Arr[T]): Arr[R] = {
    f(params)
  }

  override def aggregate(params: Arr[T], res: Buffer[R]): Buffer[R] = {
//    res ++= f(params)
    BufUtils.combine(res, f(params))
  }
}


/**
 * function apply on each partition of data with awareness of the global index
 * @param f
 * @tparam T input type
 * @tparam R return type
 */
class ParMapWithIndexFunc [T:ClassTag,R:ClassTag](val f: (Long, Arr[T]) => Arr[R])  extends ParallelFunction[T,R] {


  val dependency = FullDep


  override def apply(params: Arr[T]): Arr[R] = {
    val idx = getTaskContext().taskIdx
    f(idx, params)
  }

  override def aggregate(params: Arr[T], res: Buffer[R]): Buffer[R] = {
    //    res ++= f(params)
    val idx = getTaskContext().taskIdx
    BufUtils.combine(res, f(idx, params))
  }
}


class ParFindByFunc[T:ClassTag](val f: T=> Boolean)  extends ParallelFunction[T,T] {

  override val dependency: FuncDependency = PartialDep

  override def aggregate(params: Arr[T], res: Buffer[T]): Buffer[T] = {
//    res ++= this.apply(params)
    BufUtils.combine(res, apply(params))
  }

  override def apply(params: Arr[T]): Arr[T] = {
    params.filter(f)
  }
}



class ParReduceFunc[T:ClassTag ,R >:T :ClassTag](val f: (R, R) => R)  extends ParallelFunction[T, R]{

  val dependency = FullDep

  override def apply(params: Arr[T]): Arr[R] = {
    Arr(params.reduce(f))
  }

  override def aggregate(params: Arr[T], res: Buffer[R]): Buffer[R] = {
    val elems = if(res.isEmpty) params.reduce(f)
     else params.fold(res.head)(f)
    Buf(elems)
  }
}

class ParFoldFunc[T:ClassTag, R:ClassTag](z:R)(val f: (R, T) => R)  extends ParallelFunction[T, R] {

  val dependency = FullDep

  override def apply(params: Arr[T]): Arr[R] = {
    Arr(params.foldLeft(z)(f))
  }

  override def aggregate(params: Arr[T], res: Buffer[R]): Buffer[R] = {
    Buf(params.foldLeft(res.head)(f))
  }

}


class ParGroupByFunc[T: ClassTag, K: ClassTag](val f: T => K) extends ParallelFunction[T,(K, Iterable[T])]  {

  val dependency = FullDep

  override def apply(params: Arr[T]): Arr[(K, Iterable[T])] = {
    params.toIterable.groupBy(f).toIterator
  }


  override def aggregate(params: Arr[T], res: Buffer[(K, Iterable[T])]): Buffer[(K, Iterable[T])] = { // 40% faster than non-optimized one
//    val tempMap = res
    val tempMap = HashMap.empty[K, Iterable[T]] ++= res
    params foreach {elem =>
      val k = f(elem)
      if(tempMap.contains(k)){
        val v = tempMap.apply(k)
//        tempMap.update(k, v += elem)
        tempMap.update(k, BufUtils.add(v.asInstanceOf[Buf[T]],elem))
      } else {
        tempMap += k -> Buf(elem)
      }
    }

    tempMap.toBuffer
  }


  @deprecated("replaced by follow up aggregator", "0.0.1")
  def aggregateOld(params: Arr[T], res: Buffer[(K, Iterable[T])]): Buffer[(K, Iterable[T])] = {
    val tempMap = HashMap.empty[K, Iterable[T]]
    res foreach { e =>
      tempMap += e._1 -> e._2
    }
    params foreach {elem =>
      val k = f(elem)
      if(tempMap.contains(k)){
        val v = tempMap.apply(k)
        //        tempMap.update(k, v += elem)
        tempMap.update(k, BufUtils.add(v.asInstanceOf[Buf[T]],elem))
      } else {
        tempMap += k -> Buf(elem)
      }
    }
    tempMap.toBuffer
  }

}

class ParReduceBy[T:ClassTag, K :ClassTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)] {


  val dependency = FullDep

  override def apply(params: Arr[T]): Arr[(K, T)] = {
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
    tempMap.toIterator
  }

  override def aggregate(params: Arr[T], res: Buffer[(K, T)]): Buffer[(K, T)] = {
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
    tempMap.toBuffer
  }
}




class ParGroupByAggregation[T:ClassTag, K:ClassTag, R : ClassTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends ParallelFunction[T,(K,R)] {

  val dependency = FullDep

  override def apply(params: Arr[T]): Arr[(K, R)] = {
    params.toSeq.groupBy(fk).mapValues(_.map(t).reduce(fr)).toIterator
  }

  override def aggregate(params: Arr[T], res: Buffer[(K, R)]): Buffer[(K, R)] = {
    val mapRes = params.toSeq.groupBy(fk).mapValues(_.map(t).reduce(fr))
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

  override def apply(params: Arr[T]): Arr[T] = {
    params
  }

  override def aggregate(params: Arr[T], res: Buffer[T]): Buffer[T] = {
//    res ++= params
    BufUtils.combine(res, params)
  }
}

class FlattenFunc[T: ClassTag] extends ParallelFunction[T,T] {

  val dependency = PartialDep

  override def apply(params: Arr[T]): Arr[T] = {
    params
  }

  override def aggregate(params: Arr[T], res: Buffer[T]): Buffer[T] = {
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

  override def apply(params: Arr[T]): Arr[T] = {
    params
  }

  override def aggregate(params: Arr[T], res: Buffer[T]): Buffer[T] = {
//    res ++= params
    BufUtils.combine(res, params)
  }

}



class SortFunc[T : ClassTag](val sortInMerge:Boolean = false)
                            (implicit ordering: Ordering[T])
  extends ParallelFunction[T,T] with Aggregator[Arr[T], Buf[T]]{

  override val dependency: FuncDependency = FullDep

  @transient
  private var buffer: Array[T] = Array.empty[T]

  override def apply(params: Arr[T]): Arr[T] = {
    if(classTag[T] == classTag[Int]){
      val array = params.toArray.asInstanceOf[Array[Int]]
      Sorting.quickSort(array)
      array.toIterator.asInstanceOf[Arr[T]]
    } else {
      val array = params.toArray
//      Sorting.quickSort(array)
      Sorting.stableSort(array)
      array.toIterator
    }
  }

  override def aggregate(params: Arr[T], sorted: Buffer[T]): Buffer[T] = {
    //todo change to support AnyVal
    classTag[T] match {
      case ClassTag.Int =>
//        println("sorting array of Int")
        val array = params.toArray.asInstanceOf[Array[Int]]
        //if params has not been sorted
        if(sortInMerge)
          Sorting.quickSort(array)
        val resArray = sorted.toArray.asInstanceOf[Array[Int]]
        //merge sorted sequences
        SortingUtils.mergeSorted(resArray, array).toBuffer.asInstanceOf[Buffer[T]]
      case other:Any =>
//        println("sorting array of Any")
        val array = params.toArray
        //if params has not been sorted
        if(sortInMerge)
          Sorting.quickSort(array)
        val resArray = sorted.toArray
        //merge sorted sequences
        SortingUtils.mergeSorted(resArray, array).toBuffer
    }
  }

  def aggregateSorting(params: Arr[T], res: Buffer[T]): Buffer[T] = {
    //assume both res and params have not been sorted
    //todo change to support AnyVal
    classTag[T] match {
      case ct:ClassTag[Int] =>
        val array = params.toArray.asInstanceOf[Array[Int]]
        //        Sorting.quickSort(array)
        val resArray = res.toArray.asInstanceOf[Array[Int]]
        val newRes = Array.concat(resArray, array)
        Sorting.quickSort(newRes)
        newRes.toBuffer.asInstanceOf[Buffer[T]]
      case other:Any =>
        val array = params.toArray
        //        Sorting.quickSort(array)
        val resArray = res.toArray
        val newRes = Array.concat(resArray, array)
        Sorting.quickSort(newRes)
        newRes.toBuffer
    }
  }

  override def init(zero: Buf[T]): Unit = {
    buffer = zero.toArray
  }

  override def result: Buf[T] = {
    if (sortInMerge) {
      val bufArray = buffer
      if (classTag[T] == classTag[Int]) {
        Sorting.quickSort(bufArray.asInstanceOf[Array[Int]])
      } else {
        Sorting.stableSort(bufArray)
      }
      bufArray.toBuffer
    } else
      buffer.toBuffer
  }

  override def aggregate(params: Arr[T]): Unit = {
    if (sortInMerge) {
      buffer ++= params
    } else {
      buffer = SortingUtils.mergeSorted(params.toArray, buffer)
    }
  }

}



object ParallelFunction {

  val featureMapping:Map[Class[_ <: ParallelFunction[_,_]], Seq[FunctionFeature]] = Map(
    classOf[ParFindByFunc[_]] -> Seq(Pruning),
    classOf[ParGroupByFunc[_,_]] -> Seq(Aggregation),
    classOf[ParFoldFunc[_,_]] -> Seq(Aggregation, Pruning),
    classOf[ParReduceFunc[_,_]] -> Seq(Aggregation, Pruning),
    classOf[ParReduceBy[_,_]] -> Seq(Aggregation, Pruning),
    classOf[ParMapFunc[_,_]] -> Seq(PureParallel),
    classOf[ParMapAllFunc[_,_]] -> Seq(PureParallel)
  )

  def hasFeature(func:ParallelFunction[_,_], feature:FunctionFeature): Boolean = {
    featureMapping.contains(func.getClass) &&
      featureMapping(func.getClass).contains(feature)
  }



}