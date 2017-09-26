package org.hdm.core.math

import breeze.linalg.DenseVector
import breeze.math.Semiring
import org.hdm.core.context.{HDMEntry, HDMContext}
import HDMContext._
import HDMRowMatrix._
import org.hdm.core.model.HDM

import scala.reflect.ClassTag
import scala.{specialized => types}

/**
  * Created by tiantian on 4/05/16.
  */
class HDMRowMatrix[@types(Double, Int, Float, Long) T: ClassTag](val self: HDM[(Long, DenseVector[T])])
                                                                (implicit val e: Numeric[T],
                                                             val l: Semiring[T],
                                                             val parallelism: Int,
                                                             @transient val hDMEntry: HDMEntry)
                                                             extends Serializable with MatrixLike[T] {

  def mapRow[@types(Double, Int, Float, Long) U: ClassTag](f: DenseVector[T] => DenseVector[U])
                                                          (implicit eu: Numeric[U]): HDM[(Long, DenseVector[U])] = {
    self.mapValues(f)
  }

  def mapElem[@types(Double, Int, Float, Long) U: ClassTag](f: T => U)
                                                           (implicit eu: Numeric[U]): HDM[(Long, DenseVector[U])] = {
    self.mapValues(v => v.map(f))
  }

  def zipMap(hdv: HDM[(Long, T)], f: (DenseVector[T], T) => DenseVector[T]):HDM[(Long, DenseVector[T])] = {
    self.joinByKey(hdv).mapValues(tup => f(tup._1, tup._2))
  }

  def dot(nv: DenseVector[T]): HDVector[T] = {
    new HDVector(self.mapValues(v => v.dot(nv)))
  }

  def reduceRow(op: (DenseVector[T], DenseVector[T]) => DenseVector[T]): DenseVector[T] = {
    self.map(_._2).reduce(op).collect().next()
  }

  def reduceColumn(op: (T, T) => T): HDM[(Long, T)] = {
    self.mapValues(v => v.fold(e.zero)(op))
  }

  def norm = {
    val eu = e
    val reduceFunc = (v1: DenseVector[T], v2: DenseVector[T]) =>
      (v1.asInstanceOf[DenseVector[Double]] + v2.asInstanceOf[DenseVector[Double]]).asInstanceOf[DenseVector[T]]

    val sqrtVector = this.mapElem(v => eu.times(v, v))
      .reduceRow(reduceFunc)
      .map(d => 1 / Math.sqrt(eu.toDouble(d)))
    val meanVector = this.reduceRow(reduceFunc)
      .map(d => e.toDouble(d) / self.count().collect().next())
    this.mapRow(v =>
      (v.map(e.toDouble(_)) - meanVector) :* sqrtVector
    )
  }

  def sumColumn(): HDM[(Long, T)] = {
    this.reduceColumn(e.plus(_, _))
  }

  def sumRow: DenseVector[T] = {
    this.reduceRow(_ + _)
  }

  def sum: T = {
    this.sumColumn().sum
  }

  // operations for N1Analysis

  def numRows(): Int = {
    self.count().collect().next()
  }


  def numColumns(): Int = ???

  def column(idx: Int): DenseVector[T] = {
    self.filter(_._1 == idx).collect().next()._2
  }

  //todo check the correctness and optimize the performance
  def mapColumn[@types(Double, Int, Float, Long) U: ClassTag](f: HDVector[T] => HDVector[U])(implicit eu: Numeric[U], hDMEntry: HDMEntry) = {
    self.cache()
    val vLen = numRows
    for (i <- 0 until vLen) yield {
      val nHDM = self.mapValues(dv => dv(i))
      f(new HDVector(nHDM))
    }
  }

  def *:(vector:HDVector[Double]): DenseVector[Double] = {
    this.asInstanceOf[HDMRowMatrix[Double]].zipMap(vector.self, (vec, d) => vec * d).map(_._2).reduce(_ + _).collect().next()
  }

  def *(vector:DenseVector[T]) = {
    dot(vector)
  }

  def vertical(another:HDMRowMatrix[T]):HDMRowMatrix[T] = {
    val data1 = this.self
    val data2 = another.self
    val nData = data1.joinByKey(data2)
    nData.mapValues(tup => DenseVector(tup._1.data ++ tup._2.data))
  }

  def cache():HDMRowMatrix[T] ={
    self.cache()
  }

  override def apply[U](func: (T) => U): MatrixLike[U] = ???

  override def submatrix(vectorCon: (DenseVector[T]) => Boolean): MatrixLike[T] = ???

  override def multiply(denseVector: DenseVector[T]): MatrixLike[T] = ???

  override def multiply(matrix: MatrixLike[T]): MatrixLike[T] = ???

  override def submmatrix(idxCon: (Long) => Boolean): MatrixLike[T] = ???

  override def times(value: T): MatrixLike[T] = ???

  override def add(denseVector: DenseVector[T]): MatrixLike[T] = ???

  override def add(matrix: MatrixLike[T]): MatrixLike[T] = ???

  override def add(value: T): MatrixLike[T] = ???

}

object HDMRowMatrix {

  def vertical[T](m1:HDMRowMatrix[T], m2:HDMRowMatrix[T]):HDMRowMatrix[T]= {
    m1.vertical(m2)
  }

  implicit def hdmToMatrix[@types(Double, Int, Float, Long) T: ClassTag](hdm: HDM[(Long, DenseVector[T])])
                                                                        (implicit e: Numeric[T],
                                                                         l: Semiring[T],
                                                                         parallelism: Int,
                                                                         hDMEntry: HDMEntry): HDMRowMatrix[T] = {
    new HDMRowMatrix(hdm)
  }

  implicit def hdmToVector[@types(Double, Int, Float, Long) T: ClassTag](hdm: HDM[(Long, T)])
                                                                        (implicit e: Numeric[T]): HDVector[T] = {
    new HDVector(hdm)
  }
}
