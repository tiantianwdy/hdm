package org.nicta.wdy.hdm.functions

import org.nicta.wdy.hdm.model.{DDM, Local, HDM, DFM}
import org.nicta.wdy.hdm.io.Path
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.WeakTypeTag


/**
 * Created by Tiantian on 2014/12/16.
 */
abstract class ParallelFunction [I:ClassTag, R :ClassTag] extends SerializableFunction[Seq[I], Seq[R]] {


  def andThen[U:ClassTag](func: ParallelFunction[R, U]): ParallelFunction[I, U] = {
    val f = (seq:Seq[I]) => func(this.apply(seq))
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

object ParallelFunction {


}