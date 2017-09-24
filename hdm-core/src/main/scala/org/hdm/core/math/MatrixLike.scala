package org.hdm.core.math

import breeze.linalg.DenseVector
import scala.{specialized => types}

/**
  * Created by tiantian on 4/05/16.
  */
trait MatrixLike[@types(Double, Int, Float, Long) T] extends VectorLike[VectorLike[T]] {

  def apply[U](func: T => U): MatrixLike[U]

  def multiply(denseVector: DenseVector[T]): MatrixLike[T]

  def multiply(matrix: MatrixLike[T]): MatrixLike[T]

  def times(value: T): MatrixLike[T]

  def dot(denseVector: DenseVector[T]): VectorLike[T]

  def add(denseVector: DenseVector[T]): MatrixLike[T]

  def add(matrix: MatrixLike[T]): MatrixLike[T]

  def add(value: T): MatrixLike[T]

  def submatrix(vectorCon: DenseVector[T] => Boolean): MatrixLike[T]

  def submmatrix(idxCon: Long => Boolean): MatrixLike[T]

  def numRows():Int

  def numColumns():Int

}
