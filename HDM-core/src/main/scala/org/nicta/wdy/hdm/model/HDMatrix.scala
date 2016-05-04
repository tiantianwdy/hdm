package org.nicta.wdy.hdm.model

import breeze.linalg.{Vector, DenseVector}
import org.nicta.wdy.hdm.executor.HDMContext._
import org.nicta.wdy.hdm.math.MatrixLike
import org.nicta.wdy.hdm.model.HDMatrix._

/**
 * Created by tiantian on 4/05/16.
 */
class HDMatrix(self:HDM[(Long, DenseVector[Double])]) extends Serializable with MatrixLike{

 def mapRow(f:DenseVector[Double] => DenseVector[Double]):HDM[(Long, DenseVector[Double])] = {
   self.mapValues(f)
 }

  def update(f: Double => Double):HDM[(Long, DenseVector[Double])] = {
    self.mapValues(v => v.map(f))
  }


  def dot(nv:DenseVector) = {
    self.mapValues(v => v.dot(nv))
  }

  def reduceColumn(op:(DenseVector[Double], DenseVector[Double]) => DenseVector[Double]):DenseVector[Double] = {
    self.map(_._2).reduce(op).collect().next()
  }

  def norm = {
    val sqrtVector = this.update(v => v * v).reduceColumn( (v1, v2) => v1 + v2).map(1/Math.sqrt(_))
    val meanVector = this.reduceColumn((v1, v2) => v1 + v2).map( d => -d / self.count().collect().next())
    this.mapRow(v =>
      (v + meanVector) *  sqrtVector
    )
  }


}

object HDMatrix {

  implicit def hdmToMatrix(hdm:HDM[(Long, DenseVector[Double])]):HDMatrix = {
    new HDMatrix(hdm)
  }
}
