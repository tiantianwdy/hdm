package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm.executor.{KeepPartitioner, HashPartitioner}
import org.nicta.wdy.hdm.functions._

import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/12/11.
 */
class KvHDM[T:ClassTag, K:ClassTag,V:ClassTag](self:ParHDM[T,(K,V)]) extends Serializable{

  def mapValues[R:ClassTag](f: V => R):ParHDM[(K,V), (K,R)] = {
//    self.map(t => (t._1, f(t._2)))
    new DFM[(K, V),(K, R)](children = Seq(self), dependency = OneToOne, func = new MapValues[V,K,R](f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, R)](1))

  }

  def mapKey[NK:ClassTag] (f: K => NK):ParHDM[(K,V), (NK,V)] = {
//    self.map(t => (f(t._1), t._2))
    new DFM[(K, V),(NK, V)](children = Seq(self), dependency = OneToOne, func = new MapKeys[V,K,NK](f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(NK, V)](1))

  }

  def reduceByKey(f: (V,V)=> V): ParHDM[_, (K,V)] = {
    val pFunc = (t:(K, V)) => t._1.hashCode()
    val mapAll = (elems:Seq[(K,V)]) => {
      elems.groupBy(_._1).mapValues(_.map(_._2).reduce(f)).toSeq
    }
    val parallel = new DFM[(K,V), (K,V)](children = Seq(self), dependency = OneToN, func = new ReduceByKey(f), distribution = self.distribution, location = self.location, keepPartition = false, partitioner = new HashPartitioner(4, pFunc))
//    val aggregate = (elems:Seq[(K,V)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(f)).toSeq
    new DFM[(K, V),(K, V)](children = Seq(parallel), dependency = NToOne, func = new ReduceByKey(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))

  }

  def findByKey(f: K => Boolean): ParHDM[_, (K,V)] = {
    if(self.dependency == NToOne && self.func.isInstanceOf[ParGroupByFunc[_,K]]){
      val head = self.children.head.asInstanceOf[ParHDM[_, T]]
      val gb = self.func.asInstanceOf[ParGroupByFunc[T, K]]
      val fk:T => Boolean = f.compose(gb.f)
      val filtered = self.children.map{ c =>
          c.asInstanceOf[ParHDM[_, T]]
          .copy(keepPartition = true, dependency = OneToOne, partitioner = new KeepPartitioner[T](1))
          .filter{e => fk(e)}
          .copy(keepPartition = head.keepPartition, dependency = head.dependency, partitioner = head.partitioner)
      }
      self.asInstanceOf[ParHDM[T, (K,V)]].copy(children = filtered).asInstanceOf[ParHDM[_, (K,V)]]
    } else
      new DFM[(K, V),(K, V)](children = Seq(self), dependency = OneToOne, func = new FindByKey(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))
  }


  def findByValue(f: V=> Boolean): ParHDM[_, (K,V)] = {
    new DFM[(K, V),(K, V)](children = Seq(self), dependency = OneToOne, func = new FindByValue(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))

  }

  def swap():ParHDM[(K,V), (V,K)] ={
    self.map(t => (t._2, t._1))
  }

  def join[U:ClassTag](hdm:ParHDM[_, (K, U)]):ParHDM[_,(K,(U,V))] = ???
    // val input1 = hdm.partitionByKey
    // val input2 = this.partitionByKey
    // val new DoubleInputHDM(input1, input2).mapPartition()


}


class GroupedSeqHDM[K:ClassTag,V:ClassTag](self:ParHDM[_,(K, Iterable[V])]) extends Serializable{
  
  def mapValuesByKey[R:ClassTag](f: V => R):ParHDM[(K, Iterable[V]), (K, Iterable[R])] = {
    val func = (v: Iterable[V]) => v.map(f)
    new DFM[(K, Iterable[V]),(K, Iterable[R])](children = Seq(self), dependency = OneToOne, func = new MapValues[Iterable[V],K,Iterable[R]](func), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, Iterable[R])](1))

  }

  def findValuesByKey(f: V => Boolean):ParHDM[(K, Iterable[V]), (K, Iterable[V])] = {
    new DFM[(K, Iterable[V]),(K, Iterable[V])](children = Seq(self), dependency = OneToOne, func = new FindValuesByKey(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, Iterable[V])](1))
  }

  def reduceValues(f :(V,V) => V): ParHDM[(K,Iterable[V]), (K,V)] = {
    new DFM[(K, Iterable[V]),(K, V)](children = Seq(self),
      dependency = OneToOne,
      func = new MapValues[Iterable[V],K,V](_.reduce(f)),
      distribution = self.distribution,
      location = self.location,
      keepPartition = true,
      partitioner = new KeepPartitioner[(K, V)](1))
  }

}
