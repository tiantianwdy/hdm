package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm.executor.{KeepPartitioner, MappingPartitioner}
import org.nicta.wdy.hdm.functions.ParMapAllFunc

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Created by Tiantian on 2014/12/11.
 */
class PairHDM[K:ClassTag,V:ClassTag](self:HDM[_,(K,V)]) extends Serializable{

  def mapValues[R:TypeTag](f: V => R):HDM[(K,V), (K,R)] = {
    self.map(t => (t._1, f(t._2)))
  }

  def mapKey[NK:TypeTag] (f: K => NK):HDM[(K,V), (NK,V)] = {
    self.map(t => (f(t._1), t._2))
  }

  def reduceByKey(f: (V,V)=> V): HDM[_, (K,V)] = {
    val pFunc = (t:(K, V)) => t._1.hashCode()
    val mapAll = (elems:Seq[(K,V)]) => {
      elems.groupBy(_._1).mapValues(_.map(_._2).reduce(f)).toSeq
    }
    val parallel = new DFM[(K,V), (K,V)](children = Seq(self), dependency = OneToN, func = new ParMapAllFunc(mapAll), distribution = self.distribution, location = self.location, keepPartition = false, partitioner = new MappingPartitioner(4, pFunc))
    val aggregate = (elems:Seq[(K,V)]) => elems.groupBy(e => e._1).mapValues(_.map(_._2).reduce(f)).toSeq
    new DFM[(K, V),(K, V)](children = Seq(parallel), dependency = NToOne, func = new ParMapAllFunc(aggregate), distribution = self.distribution, location = self.location, keepPartition = true, partitioner = new KeepPartitioner[(K, V)](1))

  }

  def findByKey(f: K => Boolean): HDM[_, (K,V)] = {
    self
  }

  def swap():HDM[(K,V), (V,K)] ={
    self.map(t => (t._2, t._1))
  }

}
