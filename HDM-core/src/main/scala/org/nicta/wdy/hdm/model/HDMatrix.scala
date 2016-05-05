package org.nicta.wdy.hdm.model

import breeze.math.Semiring

import scala.reflect.ClassTag
import scala.{specialized=>types}
import breeze.linalg.{Vector, DenseVector}
import org.nicta.wdy.hdm.executor.HDMContext._
import org.nicta.wdy.hdm.math.MatrixLike
import org.nicta.wdy.hdm.model.HDMatrix._

/**
 * Created by tiantian on 4/05/16.
 */
class HDMatrix[@types(Double, Int, Float, Long) T:ClassTag](self:HDM[(Long, DenseVector[T])]) (implicit e:Numeric[T], l:Semiring[T])extends Serializable with MatrixLike{

 def mapRow[@types(Double, Int, Float, Long) U:ClassTag](f:DenseVector[T] => DenseVector[U]) (implicit e:Numeric[U]):HDM[(Long, DenseVector[U])] = {
   self.mapValues(f)
 }

  def map[@types(Double, Int, Float, Long) U:ClassTag](f: T => U)(implicit e:Numeric[U]):HDM[(Long, DenseVector[U])] = {
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
    val sqrtVector = this.map(v => e.times(v, v)).reduceRow( (v1, v2) => v1 + v2).map(d => 1/Math.sqrt(e.toDouble(d)))
    val meanVector = this.reduceRow((v1, v2) => v1 + v2).map( d => e.toDouble(d) / self.count().collect().next())
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


}

object HDMatrix {

  implicit def hdmToMatrix[@types(Double, Int, Float, Long) T:ClassTag](hdm:HDM[(Long, DenseVector[T])])(implicit e:Numeric[T], l:Semiring[T]):HDMatrix[T] = {
    new HDMatrix(hdm)
  }

  implicit def hdmToVector[@types(Double, Int, Float, Long) T:ClassTag](hdm:HDM[(Long, T)])(implicit e:Numeric[T]):HDVector[T] = {
    new HDVector(hdm)
  }
}
