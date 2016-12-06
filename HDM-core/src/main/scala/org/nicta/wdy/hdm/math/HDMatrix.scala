package org.nicta.wdy.hdm.math

import breeze.linalg.DenseVector
import breeze.math.Semiring
import org.nicta.wdy.hdm.executor.HDMContext._
import HDMatrix._
import org.nicta.wdy.hdm.model.HDM

import scala.reflect.ClassTag
import scala.{specialized => types}

/**
 * Created by tiantian on 4/05/16.
 */
class HDMatrix[@types(Double, Int, Float, Long) T:ClassTag](self:HDM[(Long, DenseVector[T])]) (implicit val e:Numeric[T], val l:Semiring[T], val parallelism:Int)extends Serializable with MatrixLike{

 def mapRow[@types(Double, Int, Float, Long) U:ClassTag](f:DenseVector[T] => DenseVector[U]) (implicit eu:Numeric[U]):HDM[(Long, DenseVector[U])] = {
   self.mapValues(f)
 }

  def mapElem[@types(Double, Int, Float, Long) U:ClassTag](f: T => U)(implicit eu:Numeric[U], lu:Semiring[U]):HDM[(Long, DenseVector[U])] = {
    self.mapValues(v => v.map(f))
  }

  def zipMap(hdv:HDM[(Long, T)], f:(DenseVector[T], T) => DenseVector[T])= {
    self.joinByKey(hdv).mapValues(tup => f(tup._1, tup._2))
  }

  def dot(nv:DenseVector[T]):HDM[(Long, T)] = {
    self.mapValues(v => v.dot(nv))
  }

  def reduceRow(op:(DenseVector[T], DenseVector[T]) => DenseVector[T])(implicit parallelism:Int):DenseVector[T] = {
    self.map(_._2).reduce(op).collect().next()
  }

  def reduceColumn(op: (T, T) => T):HDM[(Long, T)] = {
    self.mapValues( v => v.fold(e.zero)(op))
  }

  def norm(implicit parallelism:Int) = {
    val eu = e
    val reduceFunc = (v1:DenseVector[T], v2:DenseVector[T]) => (v1.asInstanceOf[DenseVector[Double]] + v2.asInstanceOf[DenseVector[Double]]).asInstanceOf[DenseVector[T]]

    val sqrtVector = this.mapElem(v => eu.times(v, v))
      .reduceRow(reduceFunc)
      .map(d => 1/Math.sqrt(eu.toDouble(d)))
    val meanVector = this.reduceRow(reduceFunc)
      .map(d => e.toDouble(d) / self.count().collect().next())
    this.mapRow(v =>
      (v.map(e.toDouble(_)) - meanVector) :*  sqrtVector
    )
  }

  def sumColumn():HDM[(Long, T)] = {
    this.reduceColumn(e.plus(_ , _))
  }

  def sumRow(implicit parallelism:Int):DenseVector[T] = {
    this.reduceRow(_ + _)
  }

  def sum(implicit parallelism:Int):T = {
    this.sumColumn().sum
  }

  // operations for N1Analysis

  def numRows(implicit parallelism:Int) :Int = {
    self.count().collect().next()
  }


  def numColumns():Int = ???

  def column(idx:Int):DenseVector[T] = {
    self.filter(_._1 == idx).collect().next()._2
  }

  //todo check the correctness and optimize the performance
  def mapColumn[@types(Double, Int, Float, Long) U:ClassTag](f: HDVector[T] => HDVector[U])(implicit eu:Numeric[U]) = {
    self.cache()
    val vLen = numRows
    for(i <- 0 until vLen) yield {
      val nHDM = self.mapValues(dv => dv(i))
      f(new HDVector(nHDM))
    }
  }


}

object HDMatrix {

  implicit def hdmToMatrix[@types(Double, Int, Float, Long) T:ClassTag](hdm:HDM[(Long, DenseVector[T])])(implicit e:Numeric[T], l:Semiring[T], parallelism:Int):HDMatrix[T] = {
    new HDMatrix(hdm)
  }

  implicit def hdmToVector[@types(Double, Int, Float, Long) T:ClassTag](hdm:HDM[(Long, T)])(implicit e:Numeric[T]):HDVector[T] = {
    new HDVector(hdm)
  }
}
