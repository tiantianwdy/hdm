package org.nicta.wdy.hdm.model

import org.nicta.wdy.hdm.math.VectorLike
import scala.reflect.ClassTag
import scala.{specialized=>types}

import org.nicta.wdy.hdm.executor.HDMContext._

/**
 * Created by tiantian on 5/05/16.
 */
class HDVector[@types(Double, Int, Float, Long) T:ClassTag](self:HDM[(Long, T)])(implicit e:Numeric[T]) extends Serializable with VectorLike {

  def times(another:HDM[(Long, T)]) ={
//    self.joinBy(another, _._1, (v:(Long, T)) => v._1).mapValues( tup => implicitly[Numeric[T]].times(tup._1._2, tup._2._2))
    self.joinByKey(another).mapValues(tup => implicitly[Numeric[T]].times(tup._1, tup._2))
  }

  def add(another:HDM[(Long, T)]) ={
    self.joinByKey(another).mapValues(tup => implicitly[Numeric[T]].plus(tup._1, tup._2))
  }

  def map(f: T => T ) = {
    self.mapValues(f)
  }

  def reduce(op: (T, T) => T) = {
    self.map(_._2).reduce(op)
  }

  def multiply(d:T) = {
    map(implicitly[Numeric[T]].times(_, d))
  }

  def add(d:T) = {
    map(implicitly[Numeric[T]].plus(_, d))
  }

  def minus(d:T) = {
    map(implicitly[Numeric[T]].minus(_, d))
  }

  def negate() = {
    map(implicitly[Numeric[T]].negate(_))
  }

  def sum(implicit parallelism:Int) = {
    reduce(e.plus(_, _)).collect().next()
  }

}
