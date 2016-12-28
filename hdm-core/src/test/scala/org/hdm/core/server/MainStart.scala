package org.hdm.core.server

import org.hdm.core.executor.HDMContext

/**
 * Created by tiantian on 17/02/15.
 */
object MainStart  {

  def main(args: Array[String]): Unit ={
//    HDMContext.defaultHDMContext.startAsMaster(mode = "multi-cluster")
    HDMContext.defaultHDMContext.init(slots = 0) // start master
  }

}
