package org.nicta.wdy.hdm.math

import org.nicta.wdy.hdm.math.HDMatrix.hdmToVector
import org.nicta.wdy.hdm.executor.HDMContext._
import org.nicta.wdy.hdm.model.HDM

import scala.reflect.ClassTag
import scala.{specialized => types}

/**
 * A distributed vector implementation of HDM.
 * The elements of the vector might be distributed among multiple nodes in a HDM cluster.
 * Each element in the vector is indexed, so it is able to do random access on the vector in a distributed manner.
 * Created by tiantian on 5/05/16.
 * todo test all the operations
 */
class HDVector[@types(Double, Int, Float, Long) T:ClassTag]
              (val self:HDM[(Long, T)])(implicit e:Numeric[T])
              extends Serializable with VectorLike {

  def times(another:HDM[(Long, T)]) = {
//    self.joinBy(another, _._1, (v:(Long, T)) => v._1).mapValues( tup => implicitly[Numeric[T]].times(tup._1._2, tup._2._2))
   val numeric = e
   val fk = (d:(Long, T)) => d._1
    self.cogroup(another, fk, fk).mapValues(tup => numeric.times(tup._1.map(_._2).sum, tup._2.map(_._2).sum))
  }

  def add(another:HDM[(Long, T)]) = {
    val numeric = e
    val fk = (d:(Long, T)) => d._1
    self.cogroup(another, fk, fk).mapValues(tup => numeric.plus(tup._1.map(_._2).sum, tup._2.map(_._2).sum))
    // optimal version after implement cogroupByKey
    //    self.cogroupByKey(another).map(tri => (tri._1, numeric.plus(tri._2.sum, tri._3.sum)))
  }


  def minus(another: HDVector[T]) = {
    self.joinByKey(another.self).mapValues(tup => e.minus(tup._1, tup._2))
  }

  def mapElem(f: T => T ) = {
    self.mapValues(f)
  }

  def reduce(op: (T, T) => T) = {
    self.map(_._2).reduce(op)
  }

  def multiply(d:T) = {
    mapElem(e.times(_, d))
  }

  def add(d:T) = {
    mapElem(e.plus(_, d))
  }

  def minus(d:T) = {
    mapElem(e.minus(_, d))
  }

  def negate() = {
    mapElem(implicitly[Numeric[T]].negate(_))
  }

  def sum(implicit parallelism:Int) = {
    val numeric = e
    val res = reduce(numeric.plus(_, _)).collect()
    res.next()
  }

  def size()(implicit parallelism:Int):Int = {
    self.count().collect().next()
  }

  def inner(another: HDVector[T]) (implicit parallelism:Int):T = {
    this.times(another.self).sum(parallelism)
  }

  def concat[A <: T](another: HDVector[A])(implicit parallelism:Int) = {
    val startIdx = this.size()
    //todo check the correctness of union
    self.union(another.self.map(tup => (tup._1 + startIdx, tup._2)))
  }

  def slice(startIdx:Int, length:Int) = {
    val endIdx = startIdx + length
    self.filter(tup => (tup._1 > startIdx && tup._1 < endIdx))
  }

  def slice(startIdx:Int) = {
    self.filter(_._1 > startIdx)
  }

  // alias operations

  def + (another:HDM[(Long, T)]) = add(another)

  def + (d:T) = add(d)

  def - (d:T) = minus(d)

  def -: = negate()

  def * (another:HDM[(Long, T)]) = times(another)

  def * (d:T) = multiply(d)

}
