package org.nicta.hdm.benchmark

import org.nicta.wdy.hdm.executor.HDMContext

/**
 * Created by tiantian on 17/02/15.
 */
object MainStart  {

  def main(args: Array[String]): Unit ={

    HDMContext.init(cores = 0) // start master
  }

}
