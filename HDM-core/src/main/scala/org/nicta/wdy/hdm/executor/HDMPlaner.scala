package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.model.HDM

/**
 * Created by Tiantian on 2014/12/10.
 */
import scala.reflect.runtime.universe._

trait HDMPlaner {

  def plan(hdm:HDM[_,_]):Seq[HDM[_,_]]

}

object DefaultPlaner extends HDMPlaner{


  override def plan(hdm:HDM[_,_]):Seq[HDM[_,_]] = {
    dftAccess(hdm)
  }



  private def dftAccess(hdm:HDM[_,_]):Seq[HDM[_,_]]=  {

    if(hdm.children == null || hdm.children.isEmpty){
      Seq(hdm)
    } else {
      val subHDMs = hdm.children.map( h => dftAccess(h)).flatten
      subHDMs :+ hdm
    }
  }
}
