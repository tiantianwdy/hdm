package org.nicta.wdy.hdm.executor

import org.nicta.wdy.hdm.functions.ParUnionFunc
import org.nicta.wdy.hdm.io.{DataParser, Path}
import org.nicta.wdy.hdm.model.{DFM, DDM, HDM}

/**
 * Created by Tiantian on 2014/12/10.
 */
import scala.reflect.runtime.universe._

trait HDMPlaner {

  def plan(hdm:HDM[_,_]):Seq[HDM[_,_]]

}

object LocalPlaner extends HDMPlaner{


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

object ClusterPlaner extends HDMPlaner {


  override def plan(hdm:HDM[_,_]):Seq[HDM[_,_]] = {
    dftAccess(hdm)
  }

  private def dftAccess(hdm:HDM[_,_]):Seq[HDM[_,_]]=  {

    if(hdm.children == null || hdm.children.isEmpty){
      hdm match {
        case ddm :DDM[_] => Seq(ddm)
        case leafHdm:DFM[Path,String] =>
          val children = DataParser.explainBlocks(leafHdm.location)
          val newParent = DFM(id = leafHdm.id, children = children, func = new ParUnionFunc[String]())
          children :+ newParent
        case x => throw new Exception("unsupported hdm.")
      }
    } else {
      val subHDMs = hdm.children.map( h => dftAccess(h)).flatten
      subHDMs :+ hdm
    }
  }
  
}
