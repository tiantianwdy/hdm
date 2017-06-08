package org.hdm.core.model


import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
/**
  * Created by Tiantian on 2014/5/26.
  */
abstract class RemoteHDM[T:ClassTag, R:ClassTag](val elemsPath: String) extends ParHDM[T, R] {

//   val children = HDM.findRemoteHDM(elemsPath)

}

