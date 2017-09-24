package org.hdm.core.math

/**
  * Created by tiantian on 24/09/17.
  */
class MatrixOps[T](self:MatrixLike[T])(implicit numeric: Numeric[T]){


  def sigmoid(): MatrixLike[Double] = {
    self.apply(d => 1/(1 + Math.exp(- numeric.toDouble(d))))
  }


  def tanh(): MatrixLike[Double] = {
    self.apply{ d => Math.tanh(numeric.toDouble(d))}
  }

  def arctan(): MatrixLike[Double] = {
    self.apply(d => Math.atan(numeric.toDouble(d)))
  }



}


object MatrixOps {

  implicit def hdmToMatrixOps[T](matrix:MatrixLike[T])(implicit e: Numeric[T]): MatrixOps[T] ={
    new MatrixOps(matrix)
  }
}