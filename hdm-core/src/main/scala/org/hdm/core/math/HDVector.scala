package org.hdm.core.math

import org.hdm.core.context.HDMContext
import org.hdm.core.math.HDMatrix.hdmToVector
import HDMContext._
import org.hdm.core.model.HDM
import org.hdm.core.server.HDMServerContext

import scala.reflect.ClassTag
import scala.{specialized => types}

import java.lang.{Double => JDouble}

/**
  * A distributed vector implementation of HDM.
  * The elements of the vector might be distributed among multiple nodes in a HDM cluster.
  * Each element in the vector is indexed, so it is able to do random access on the vector in a distributed manner.
  * Created by tiantian on 5/05/16.
  * todo test all the operations
  */
class HDVector[@types(Double, Int, Float, Long) T: ClassTag]
(val self: HDM[(Long, T)])(implicit e: Numeric[T])
  extends Serializable with VectorLike {

  def times(another: HDM[(Long, T)]): HDM[(Long, T)] = {
    //    self.joinBy(another, _._1, (v:(Long, T)) => v._1).mapValues( tup => implicitly[Numeric[T]].times(tup._1._2, tup._2._2))
    val numeric = e
    val fk = (d: (Long, T)) => d._1
    self.cogroup(another, fk, fk).mapValues(tup => numeric.times(tup._1.map(_._2).sum, tup._2.map(_._2).sum))
  }


  def times(another: Iterator[T]): HDM[(Long, T)] = {
    val localVector = another.zipWithIndex.map(e => (e._2.toLong, e._1)).toSeq
    this.times(HDM.parallelize(localVector))
  }

  def add(another: HDM[(Long, T)]): HDM[(Long, T)] = {
    val numeric = e
    val fk = (d: (Long, T)) => d._1
    self.cogroup(another, fk, fk).mapValues(tup => numeric.plus(tup._1.map(_._2).sum, tup._2.map(_._2).sum))
    // optimal version after implement cogroupByKey
    //    self.cogroupByKey(another).map(tri => (tri._1, numeric.plus(tri._2.sum, tri._3.sum)))
  }

  def add(another: Iterator[T]): HDM[(Long, T)] = {
    val localVector = another.zipWithIndex.map(e => (e._2.toLong, e._1)).toSeq
    this.add(HDM.parallelize(localVector))
  }


  def minus(another: HDVector[T]): HDM[(Long, T)] = {
    self.joinByKey(another.self).mapValues(tup => e.minus(tup._1, tup._2))
  }

  def minus(another: Iterator[T]): HDM[(Long, T)] = {
    val localVector = another.zipWithIndex.map(e => (e._2.toLong, e._1)).toSeq
    this.minus(HDM.parallelize(localVector))
  }

  def mapElem(f: T => T) = {
    self.mapValues(f)
  }

  def reduce(op: (T, T) => T) = {
    self.map(_._2).reduce(op)
  }

  def multiply(d: T) = {
    mapElem(e.times(_, d))
  }

  def add(d: T) = {
    mapElem(e.plus(_, d))
  }

  def minus(d: T) = {
    mapElem(e.minus(_, d))
  }

  def negate() = {
    mapElem(implicitly[Numeric[T]].negate(_))
  }

  def sum(implicit parallelism: Int) = {
    val numeric = e
    val res = reduce(numeric.plus(_, _)).collect()
    res.next()
  }

  def size()(implicit parallelism: Int): Int = {
    self.count().collect().next()
  }

  def inner(another: HDVector[T])(implicit parallelism: Int): T = {
    this.times(another.self).sum(parallelism)
  }


  def concat[A <: T](another: HDVector[A])(implicit parallelism: Int) = {
    val startIdx = this.size()
    //todo check the correctness of union
    self.union(another.self.map(tup => (tup._1 + startIdx, tup._2)))
  }

  def slice(startIdx: Int, length: Int) = {
    val endIdx = startIdx + length
    self.filter(tup => (tup._1 > startIdx && tup._1 < endIdx))
  }

  def slice(startIdx: Int) = {
    self.filter(_._1 > startIdx)
  }

  // alias operations

  def +(another: HDM[(Long, T)]) = add(another)

  def +(d: T) = add(d)

  def -(d: T) = minus(d)

  def -: = negate()

  def *(another: HDM[(Long, T)]) = times(another)

  def *(d: T) = multiply(d)

  def toHDM() = self

}
