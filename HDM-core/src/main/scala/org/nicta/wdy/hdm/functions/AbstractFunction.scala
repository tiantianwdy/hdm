package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm._
import org.nicta.wdy.hdm.ParallelSkeletons.DnCSkeleton
import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext}
import org.nicta.wdy.hdm.model.{DDM, Local, HDM, DFM}
import org.nicta.wdy.hdm.executor.HDMContext
import scala.reflect.runtime.universe._

/**
 * Created by Tiantian on 2014/11/4.
 */
/**
 *
 * @tparam R return type
 */
trait SerializableFunction[I,R] extends  Serializable{

  def apply(params: Seq[I]) : R

  def andThen[U](func: SerializableFunction[R,U]): SerializableFunction[I, U] = ???
}



/**
 * Serializable function with 1 parameter
 * @tparam I input type
 * @tparam R return type
 */
abstract class DDMFunction_1[I,R :TypeTag]extends SerializableFunction[DDM[I], DDM[R]] {

}

/**
 * Serializable function with 2 parameters
 * @tparam T input type
 * @tparam R return type
 */
abstract class HDMFunction_2[T,R]extends SerializableFunction[HDM[_,T], HDM[T, R]]

abstract class IterableFunction_2[T,R] extends SerializableFunction[Iterator[T], Iterator[R]]   {

  override def apply(params: Seq[Iterator[T]]): Iterator[R] = ???

}




class Union[T:TypeTag] extends HDMFunction_2[T,T]{


  override def apply(params: Seq[HDM[_,T]]): HDM[T,T] = {
    new DFM[T,T](params)
  }
}

abstract class DnCFunc[T,M,R:TypeTag] extends DDMFunction_1[T,R] with DnCSkeleton[HDM[_,T],HDM[T,M],HDM[M,R]] {

  import scala.concurrent._


// {
//    val seq = params(0).elements.map{ hdm =>
//      if(hdm.location == Local)  {
//         HDMContext.runTask(hdm, this.divide)
//      } else {// remote
//         HDMContext.sendFunc(hdm, this.divide)
//      }
//    }
//
//    val futures = Future.sequence(seq.asInstanceOf[Seq[Future[HDM[M]]]])
//
//    futures onFailure{
//      case e:Throwable => println(e.getMessage)
//      case _ =>
//    }
//
//    futures.result(atMost).map(conquer).reduce(merge)
//
//  }
  override def apply(params: Seq[DDM[T]]): DDM[R] = ???
}


import scala.concurrent._


class MapFunc[T,R:TypeTag](f: T=>R)  extends DDMFunction_1[T,R] {



  override def apply(params: Seq[DDM[T]]): DDM[R] = {

    DDM(params.map(p=> p.elems).flatMap(_.map(f)))
  }
  //  override val divide = (d:HDM[T]) => d.apply(f)
  //
  //  override val conquer: (HDM[R]) => HDM[R] = (d:HDM[R]) => d
  //
  //  override val merge: (HDM[R], HDM[R]) => HDM[R] = (d1, d2) => new Union().apply(Seq(d1,d2))

}


class ReduceFunc[T,R >:T :TypeTag](f: (R, R) => R)  extends DDMFunction_1[T,R] {

  override def apply(params: Seq[DDM[T]]): DDM[R] = {
    DDM(Seq(params.flatMap(p => p.elems).reduce(f)))
  }
}

class FoldFunc[T,R :TypeTag](z:R)(f: (R, T) => R)  extends DDMFunction_1[T,R] {

  override def apply(params: Seq[DDM[T]]): DDM[R] = {
    DDM(Seq(params.flatMap(p => p.elems).foldLeft(z)(f)))
  }
}


class GroupByFunc[T:TypeTag,K :TypeTag](f: T => K) extends DDMFunction_1[T,(K,Seq[T])] {

  override def apply(params: Seq[DDM[T]]): DDM[(K, Seq[T])] = {
    DDM(params.flatMap(p=> p.elems).groupBy(f).toSeq)
  }
}

class ReduceByKey[T:TypeTag, K :TypeTag](fk: T=> K, fr: (T, T) => T) extends DDMFunction_1[T,(K,T)]{

  override def apply(params: Seq[DDM[T]]): DDM[(K, T)] = {
    DDM(params.flatMap(p => p.elems).groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr))))
  }
}

class GroupFoldByKey[T:TypeTag, K:TypeTag, R : TypeTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends DDMFunction_1[T,(K,R)]{

  override def apply(params: Seq[DDM[T]]): DDM[(K, R)] = {
    DDM(params.flatMap(p => p.elems).groupBy(fk).mapValues(_.map(t).reduce(fr)).toSeq)
  }
}

class UnionFunc[T :TypeTag]()  extends DDMFunction_1[T,T] {

  override def apply(params: Seq[DDM[T]]): DDM[T] = {
    DDM(params.map(p => p.elems).flatten.seq)
  }

}



