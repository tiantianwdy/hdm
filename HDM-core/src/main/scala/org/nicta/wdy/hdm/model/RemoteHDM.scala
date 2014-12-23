package org.nicta.wdy.hdm.model


import scala.reflect.runtime.universe._
/**
  * Created by Tiantian on 2014/5/26.
  */
abstract class RemoteHDM[T:TypeTag, R:TypeTag](val elemsPath: String) extends HDM[T, R] {

//   val children = HDM.findRemoteHDM(elemsPath)

}

