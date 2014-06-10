package org.nicta.wdy.hdm.data

/**
 * Created by Tiantian on 2014/5/27.
 */
trait Context {

}


object HDMContext extends Context{

  lazy val HDM_MANAGER = new HDMDataManager

  def getHDMManager = HDM_MANAGER
}
