package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.model.{DDM, Local, HDM, DFM}
import scala.reflect.runtime.universe._
import org.nicta.wdy.hdm.executor.Partitioner

/**
 * Created by Tiantian on 2014/12/16.
 */
abstract class ParallelFunction [I:TypeTag, R :TypeTag]extends SerializableFunction[Seq[I], Seq[R]]

class ParMapFunc [T:TypeTag,R:TypeTag](f: T=>R)  extends ParallelFunction[T,R] {

  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    params.flatMap(t => t.map(f))
  }
}

class ParReduceFunc[T:TypeTag ,R >:T :TypeTag](f: (R, R) => R)  extends ParallelFunction[T,R] {

  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    val res = params.flatten.reduce(f)
    Seq(res)
  }
}

class ParFoldFunc[T:TypeTag, R:TypeTag](z:R)(f: (R, T) => R)  extends ParallelFunction[T,R] {


  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    Seq(params.flatten.foldLeft(z)(f))
  }

}


class ParGroupByFunc[T: TypeTag, K: TypeTag](f: T => K) extends ParallelFunction[T,(K,Seq[T])] {


  override def apply(params: Seq[Seq[T]]): Seq[(K, Seq[T])] = {
    params.flatMap(p=> p.groupBy(f)).toSeq
  }

}

class ParReduceByKey[T:TypeTag, K :TypeTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)]{


  override def apply(params: Seq[Seq[T]]): Seq[(K, T)] = {
    params.flatMap(p => p.groupBy(fk)).toSeq.map(d => (d._1, d._2.reduce(fr)))
  }

}

class ParGroupFoldByKey[T:TypeTag, K:TypeTag, R : TypeTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends ParallelFunction[T,(K,R)]{


  override def apply(params: Seq[Seq[T]]): Seq[(K, R)] = {
    params.flatMap(p => p.groupBy(fk).mapValues(_.map(t).reduce(fr))).toSeq
  }

}

class ParUnionFunc[T: TypeTag]()  extends ParallelFunction[T,T] {


  override def apply(params: Seq[Seq[T]]): Seq[T] = {
    params.flatten
  }


}
