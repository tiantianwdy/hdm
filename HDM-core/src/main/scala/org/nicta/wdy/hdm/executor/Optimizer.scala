package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.model.HDM

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
  def optimize(hdm:HDM[_,_]): HDM[_,_]

}
