package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.model.{DFM, HDM}

import scala.reflect.ClassTag

/**
 * Created by Tiantian on 2014/12/10.
 */
trait Optimizer {

  /**
   * optimize the structure of HDM
   *
   * @param hdm  input HDM
   * @return     optimized HDM
   */
  def optimize(hdm:Seq[HDM[_,_]]): Seq[HDM[_,_]]

}

object Optimizer {

 def combine[I:ClassTag, M:ClassTag, R:ClassTag](first:HDM[I,M], second:HDM[M, R] ):HDM[I,R] = {
    val cFunc = first.func.andThen(second.func)
    DFM(id = second.id, children = first.children, func = cFunc, partitioner = second.partitioner, parallelism = first.parallelism)
 }

}
