package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.model.{DDM, Local, HDM, DFM}
import org.nicta.wdy.hdm.io.Path
import scala.reflect.runtime.universe.WeakTypeTag


/**
 * Created by Tiantian on 2014/12/16.
 */
abstract class ParallelFunction [I:WeakTypeTag, R :WeakTypeTag] extends SerializableFunction[Seq[I], Seq[R]]


class ParMapFunc [T:WeakTypeTag,R:WeakTypeTag](f: T=>R)  extends ParallelFunction[T,R] {

  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    params.flatMap(t => t.map(f))
  }
}

class ParMapAllFunc [T:WeakTypeTag,R:WeakTypeTag](f: Seq[T]=>Seq[R])  extends ParallelFunction[T,R] {

  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    params.flatMap(f).toSeq
  }
}


class ParReduceFunc[T:WeakTypeTag ,R >:T :WeakTypeTag](f: (R, R) => R)  extends ParallelFunction[T, R] {

  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    val res = params.map{s =>
      var (cur, remain) = s.splitAt(1)
      var pre = cur.reduce(f)
      while(!remain.isEmpty && remain.size > 10000) {
        val d = remain.splitAt(10000)
        cur = d._1
        remain = d._2
        pre = cur.fold(pre)(f)
        println(remain.size)
      }
      if(remain.size > 0) pre = remain.fold(pre)(f)
      pre
    }
    res
  }
}

class ParFoldFunc[T:WeakTypeTag, R:WeakTypeTag](z:R)(f: (R, T) => R)  extends ParallelFunction[T, R] {


  override def apply(params: Seq[Seq[T]]): Seq[R] = {
    Seq(params.flatten.foldLeft(z)(f))
  }

}


class ParGroupByFunc[T: WeakTypeTag, K: WeakTypeTag](f: T => K) extends ParallelFunction[T,(K,Seq[T])] {


  override def apply(params: Seq[Seq[T]]): Seq[(K, Seq[T])] = {
    params.flatMap(p=> p.groupBy(f)).toSeq
  }

}

class ParReduceByKey[T:WeakTypeTag, K :WeakTypeTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)]{


  override def apply(params: Seq[Seq[T]]): Seq[(K, T)] = {
    params.flatMap(p => p.groupBy(fk)).toSeq.map(d => (d._1, d._2.reduce(fr)))
  }

}

class ParGroupFoldByKey[T:WeakTypeTag, K:WeakTypeTag, R : WeakTypeTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends ParallelFunction[T,(K,R)]{


  override def apply(params: Seq[Seq[T]]): Seq[(K, R)] = {
    params.flatMap(p => p.groupBy(fk).mapValues(_.map(t).reduce(fr))).toSeq
  }

}

class ParUnionFunc[T: WeakTypeTag]()  extends ParallelFunction[T,T] {


  override def apply(params: Seq[Seq[T]]): Seq[T] = {
    params.flatten
  }


}

class FlattenFunc[T: WeakTypeTag] extends ParallelFunction[T,T]{

  override def apply(params: Seq[Seq[T]]): Seq[T] = {
    params.flatten
  }
}
