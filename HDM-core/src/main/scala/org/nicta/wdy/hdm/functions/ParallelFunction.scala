package org.nicta.wdy.hdm.functions


import scala.reflect.ClassTag
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


  def andThen[U:ClassTag](func: ParallelFunction[R, U]): ParallelFunction[T, U] = {
    val f = (seq:Seq[T]) => func(this.apply(seq))
    new ParMapAllFunc(f)
  }

  def compose[I:ClassTag](func: ParallelFunction[I, T]): ParallelFunction[I, R] = {
    val f = (seq:Seq[I]) => this.apply(func.apply(seq))
    new ParMapAllFunc(f)
  }

}


class ParMapFunc [T:ClassTag,R:ClassTag](f: T=>R)  extends ParallelFunction[T,R] {

  override def apply(params: Seq[T]): Seq[R] = {
    params.map(f)
  }
}

class ParMapAllFunc [T:ClassTag,R:ClassTag](f: Seq[T]=>Seq[R])  extends ParallelFunction[T,R] {

  override def apply(params: Seq[T]): Seq[R] = {
    f(params)
  }
}


class ParReduceFunc[T:ClassTag ,R >:T :ClassTag](f: (R, R) => R)  extends ParallelFunction[T, R] {

  override def apply(params: Seq[T]): Seq[R] = {
    Seq(params.reduce(f))
  }
}

class ParFoldFunc[T:ClassTag, R:ClassTag](z:R)(f: (R, T) => R)  extends ParallelFunction[T, R] {


  override def apply(params: Seq[T]): Seq[R] = {
    Seq(params.foldLeft(z)(f))
  }

}


class ParGroupByFunc[T: ClassTag, K: ClassTag](f: T => K) extends ParallelFunction[T,(K,Seq[T])] {


  override def apply(params: Seq[T]): Seq[(K, Seq[T])] = {
    params.groupBy(f).toSeq
  }

}

class ParReduceByKey[T:ClassTag, K :ClassTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)]{


  override def apply(params: Seq[T]): Seq[(K, T)] = {
    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr)))
  }

}

class ParGroupFoldByKey[T:ClassTag, K:ClassTag, R : ClassTag] (fk: T=> K, t: T=> R, fr: (R, R) => R) extends ParallelFunction[T,(K,R)]{


  override def apply(params: Seq[T]): Seq[(K, R)] = {
    params.groupBy(fk).mapValues(_.map(t).reduce(fr)).toSeq
  }

}

class ParUnionFunc[T: ClassTag]()  extends ParallelFunction[T,T] {


  override def apply(params: Seq[T]): Seq[T] = {
    params
  }


}

class FlattenFunc[T: ClassTag] extends ParallelFunction[T,T]{

  override def apply(params: Seq[T]): Seq[T] = {
    params
  }
}

class NullFunc[T: ClassTag] extends ParallelFunction[T,T]{

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
}

object ParallelFunction {


}