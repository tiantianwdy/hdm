package org.hdm.core.context

/**
 * Created by tiantian on 27/04/16.
 */
case class AppContext(var appName: String = "defaultApp",
                 var version: String = "0.0.1",
                 var masterPath: String = "") extends Serializable {

  def setMasterPath(path:String): Unit ={
    masterPath = path
  }

}

object AppContext {

  val defaultAppContext = new AppContext
}
