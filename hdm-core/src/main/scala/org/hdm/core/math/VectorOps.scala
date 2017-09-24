package org.hdm.core.math


import HDMRowMatrix.hdmToVector
/**
  * Created by tiantian on 24/09/17.
  */
class VectorOps[T](self:HDVector[T])(implicit numeric: Numeric[T]) {

  def sigmoid(): HDVector[Double] = {
    self.mapElem(d => 1/(1 + Math.exp(- numeric.toDouble(d))))
  }


  def tanh(): HDVector[Double] = {
    self.mapElem{ d => Math.tanh(numeric.toDouble(d))}
  }

  def arctan(): HDVector[Double] = {
    self.mapElem(d => Math.atan(numeric.toDouble(d)))
  }
}


object VectorOps {

  implicit def hdmToVector[T](hdm: HDVector[T])(implicit e: Numeric[T]): VectorOps[T] ={
    new VectorOps(hdm)
  }
}