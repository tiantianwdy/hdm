package org.nicta.wdy.hdm.functions


import scala.collection.mutable
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


  def Aggregate(params:Seq[T], res:Seq[R]): Seq[R] = ???

}


class ParMapFunc [T:ClassTag,R:ClassTag](f: T=>R)  extends ParallelFunction[T,R] {

  override def apply(params: Seq[T]): Seq[R] = {
    params.map(f)
  }

  override def Aggregate(params: Seq[T], res: Seq[R]): Seq[R] = {
    params.map(f) ++ res
  }
}

class ParMapAllFunc [T:ClassTag,R:ClassTag](f: Seq[T]=>Seq[R])  extends ParallelFunction[T,R] {

  override def apply(params: Seq[T]): Seq[R] = {
    f(params)
  }

  override def Aggregate(params: Seq[T], res: Seq[R]): Seq[R] = {
    f(params) ++ res
  }
}


class ParReduceFunc[T:ClassTag ,R >:T :ClassTag](f: (R, R) => R)  extends ParallelFunction[T, R] {

  override def apply(params: Seq[T]): Seq[R] = {
    Seq(params.reduce(f))
  }

  override def Aggregate(params: Seq[T], res: Seq[R]): Seq[R] = {
    Seq(params.fold(res.reduce(f))(f))
  }
}

class ParFoldFunc[T:ClassTag, R:ClassTag](z:R)(f: (R, T) => R)  extends ParallelFunction[T, R] {


  override def apply(params: Seq[T]): Seq[R] = {
    Seq(params.foldLeft(z)(f))
  }

  override def Aggregate(params: Seq[T], res: Seq[R]): Seq[R] = {
    Seq(params.foldLeft(res.head)(f))
  }

}


class ParGroupByFunc[T: ClassTag, K: ClassTag](f: T => K) extends ParallelFunction[T,(K,Seq[T])] {


  override def apply(params: Seq[T]): Seq[(K, Seq[T])] = {
    params.groupBy(f).toSeq
  }

  override def Aggregate(params: Seq[T], res: Seq[(K, Seq[T])]): Seq[(K, Seq[T])] = {
    val temp = mutable.HashMap.empty[K,Seq[T]] ++= res //todo change to use AppendOnlyMap
    params.groupBy(f) foreach{ tup =>
      if(temp.contains(tup._1)){
        val v = temp.apply(tup._1)
        temp.update(tup._1, v ++ tup._2)
      } else {
        temp += tup
      }
    }
    temp.toSeq
  }
}

class ParReduceByKey[T:ClassTag, K :ClassTag](fk: T=> K, fr: (T, T) => T) extends ParallelFunction[T,(K,T)]{


  override def apply(params: Seq[T]): Seq[(K, T)] = {
    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr)))
  }

  override def Aggregate(params: Seq[T], res: Seq[(K, T)]): Seq[(K, T)] = {
    val tempMap = mutable.HashMap.empty[K,T] ++= res
    params.groupBy(fk).toSeq.map(d => (d._1, d._2.reduce(fr))) foreach { tup =>
      if(tempMap.contains(tup._1)){
        val v = tempMap.apply(tup._1)
        tempMap.update(tup._1, fr(v, tup._2))
      } else {
        tempMap += tup
      }
    }
    tempMap.toSeq
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

  override def Aggregate(params: Seq[T], res: Seq[T]): Seq[T] = {
    params ++ res
  }
}

class FlattenFunc[T: ClassTag] extends ParallelFunction[T,T]{

  override def apply(params: Seq[T]): Seq[T] = {
    params
  }

  override def Aggregate(params: Seq[T], res: Seq[T]): Seq[T] = {
    params ++ res
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

  override def Aggregate(params: Seq[T], res: Seq[T]): Seq[T] = {
    params ++ res
  }

}

object ParallelFunction {


}