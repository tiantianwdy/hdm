package org.hdm.core.functions

import org.hdm.core.ParallelSkeletons.DnCSkeleton
import org.hdm.core.model.{DDM, DFM, ParHDM}

import scala.reflect.ClassTag




/**
 * Serializable function with 1 parameter
 * @tparam I input type
 * @tparam R return type
 */
abstract class DDMFunction_1[I,R :ClassTag]extends SerializableFunction[Seq[DDM[_,I]], DDM[I,R]] {

  override def apply(params: Seq[DDM[_, I]]): DDM[I, R] = ???
}

/**
 * Serializable function with 2 parameters
 * @tparam T input type
 * @tparam R return type
 */
abstract class HDMFunction_2[T,R]extends SerializableFunction[Seq[ParHDM[_,T]], ParHDM[T, R]]

abstract class IterableFunction_2[T,R] extends SerializableFunction[Iterator[T], Iterator[R]]   {

  override def apply(params: Iterator[T]): Iterator[R] = ???

}




class Union[T:ClassTag] extends HDMFunction_2[T,T]{


  override def apply(params: Seq[ParHDM[_,T]]): ParHDM[T,T] = {
    new DFM[T,T](children = params, location = params.head.location, appContext = params.head.appContext)
  }
}

abstract class DnCFunc[T,M,R:ClassTag] extends DDMFunction_1[T,R] with DnCSkeleton[ParHDM[_,T],ParHDM[T,M],ParHDM[M,R]] {


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
  override def apply(params: Seq[DDM[_,T]]): DDM[T,R] = ???
}



//
//class MapFunc[T,R:ClassTag](f: T=>R)  extends DDMFunction_1[T,R] {
//
//
//
//  override def apply(params: Seq[DDM[T]]): DDM[R] = {
//
//    DDM(params.map(p=> p.elems).flatMap(_.map(f)))
//  }
  //  override val divide = (d:HDM[T]) => d.apply(f)
  //
  //  override val conquer: (HDM[R]) => HDM[R] = (d:HDM[R]) => d
  //
  //  override val merge: (HDM[R], HDM[R]) => HDM[R] = (d1, d2) => new Union().apply(Seq(d1,d2))

//}

/*
class ReduceFunc[T,R >:T :ClassTag](f: (R, R) => R)  extends DDMFunction_1[T,R] {

  override def apply(params: Seq[DDM[T]]): DDM[R] = {
    DDM(Seq(params.flatMap(p => p.elems).reduce(f)))
  }
}

class FoldFunc[T,R :ClassTag](z:R)(f: (R, T) => R)  extends DDMFunction_1[T,R] {

  override def apply(params: Seq[DDM[T]]): DDM[R] = {
    DDM(Seq(params.flatMap(p => p.elems).foldLeft(z)(f)))
  }
}


class GroupByFunc[T:ClassTag,K :ClassTag](f: T => K) extends DDMFunction_1[T,(K,Seq[T])] {

  override def apply(params: Seq[DDM[T]]): DDM[(K, Seq[T])] = {
    DDM(params.flatMap(p=> p.elems).groupBy(f).toSeq)
  }
}

class ReduceByKey[T:ClassTag, K :ClassTag](fk: T=> K, fr: (T, T) => T) extends DDMFunction_1[T,(K,T)]{

  override def apply(params: Seq[DDM[T]]): DDM[(K, T)] = {
    DDM(params.flatMap(p => p.elems).groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr))))
  }
}

class GroupFoldByKey[T:ClassTag, K:ClassTag, R : ClassTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends DDMFunction_1[T,(K,R)]{

  override def apply(params: Seq[DDM[T]]): DDM[(K, R)] = {
    DDM(params.flatMap(p => p.elems).groupBy(fk).mapValues(_.map(t).reduce(fr)).toSeq)
  }
}

class UnionFunc[T :ClassTag]()  extends DDMFunction_1[T,T] {

  override def apply(params: Seq[DDM[T]]): DDM[T] = {
    DDM(params.map(p => p.elems).flatten.seq)
  }

}*/



