package org.nicta.hdm.server

import org.nicta.wdy.hdm.executor.HDMContext

/**
 * Created by tiantian on 17/02/15.
 */
object MainStart  {

  def main(args: Array[String]): Unit ={

    HDMContext.defaultHDMContext.init(slots = 0) // start master
  }

}
