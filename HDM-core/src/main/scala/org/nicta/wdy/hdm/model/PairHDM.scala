package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm.Buf
import org.nicta.wdy.hdm.executor.{KeepPartitioner, HashPartitioner}
import org.nicta.wdy.hdm.functions._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Created by Tiantian on 2014/12/11.
 */
class PairHDM[T:ClassTag, K:ClassTag,V:ClassTag](self:HDM[T,(K,V)]) extends Serializable{

  def mapValues[R:ClassTag](f: V => R):HDM[(K,V), (K,R)] = {
//    self.map(t => (t._1, f(t._2)))
    new DFM[(K, V),(K, R)](children = Seq(self), dependency = OneToOne, func = new MapValues[V,K,R](f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, R)](1))

  }

  def mapKey[NK:ClassTag] (f: K => NK):HDM[(K,V), (NK,V)] = {
//    self.map(t => (f(t._1), t._2))
    new DFM[(K, V),(NK, V)](children = Seq(self), dependency = OneToOne, func = new MapKeys[V,K,NK](f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(NK, V)](1))

  }

  def reduceByKey(f: (V,V)=> V): HDM[_, (K,V)] = {
    val pFunc = (t:(K, V)) => t._1.hashCode()
    val mapAll = (elems:Seq[(K,V)]) => {
      elems.groupBy(_._1).mapValues(_.map(_._2).reduce(f)).toSeq
    }
    val parallel = new DFM[(K,V), (K,V)](children = Seq(self), dependency = OneToN, func = new ReduceByKey(f), distribution = self.distribution, location = self.location, keepPartition = false, partitioner = new HashPartitioner(4, pFunc))
//    val aggregate = (elems:Seq[(K,V)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(f)).toSeq
    new DFM[(K, V),(K, V)](children = Seq(parallel), dependency = NToOne, func = new ReduceByKey(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))

  }

  def findByKey(f: K => Boolean): HDM[_, (K,V)] = {
    if(self.dependency == NToOne && self.func.isInstanceOf[ParGroupByFunc[_,K]]){
      val head = self.children.head.asInstanceOf[HDM[_, T]]
      val gb = self.func.asInstanceOf[ParGroupByFunc[T, K]]
      val fk:T => Boolean = f.compose(gb.f)
      val filtered = self.children.map{ c =>
          c.asInstanceOf[HDM[_, T]]
          .copy(keepPartition = true, dependency = OneToOne, partitioner = new KeepPartitioner[T](1))
          .filter{e => fk(e)}
          .copy(keepPartition = head.keepPartition, dependency = head.dependency, partitioner = head.partitioner)
      }
      self.asInstanceOf[HDM[T, (K,V)]].copy(children = filtered).asInstanceOf[HDM[_, (K,V)]]
    } else
      new DFM[(K, V),(K, V)](children = Seq(self), dependency = OneToOne, func = new FindByKey(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))
  }


  def findByValue(f: V=> Boolean): HDM[_, (K,V)] = {
    new DFM[(K, V),(K, V)](children = Seq(self), dependency = OneToOne, func = new FindByValue(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))

  }

  def swap():HDM[(K,V), (V,K)] ={
    self.map(t => (t._2, t._1))
  }

}


class GroupedSeqHDM[K:ClassTag,V:ClassTag](self:HDM[_,(K,Buf[V])]) extends Serializable{
  
  def mapValuesByKey[R:ClassTag](f: V => R):HDM[(K,Buf[V]), (K,Buf[R])] = {
    val func = (v: Buf[V]) => v.map(f)
    new DFM[(K, Buf[V]),(K, Buf[R])](children = Seq(self), dependency = OneToOne, func = new MapValues[Buf[V],K,Buf[R]](func), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, Buf[R])](1))

  }

  def findValuesByKey(f: V => Boolean):HDM[(K,Buf[V]), (K,Buf[V])] = {
    new DFM[(K, Buf[V]),(K, Buf[V])](children = Seq(self), dependency = OneToOne, func = new FindValuesByKey(f), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, Buf[V])](1))
  }

  def reduceValues(f :(V,V) => V): HDM[(K,Buf[V]), (K,V)] = {
    new DFM[(K, Buf[V]),(K, V)](children = Seq(self),
      dependency = OneToOne,
      func = new MapValues[Buf[V],K,V](_.reduce(f)),
      distribution = self.distribution,
      location = self.location,
      keepPartition = true,
      partitioner = new KeepPartitioner[(K, V)](1))
  }

}
