package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.model.HDM

/**
 * Created by tiantian on 7/01/15.
 */
trait LogicalPlaner extends Serializable{
  def plan(hdm:HDM[_,_], parallelism:Int):Seq[HDM[_,_]]
}


/**
 *
 */
object DefaultLocalPlaner extends LogicalPlaner{


  override def plan(hdm:HDM[_,_], parallelism:Int = 4):Seq[HDM[_,_]] = {
    dftAccess(hdm, parallelism, 1)
  }



  private def dftAccess(hdm:HDM[_,_], defParallel:Int, followingParallel:Int):Seq[HDM[_,_]]=  {
    val nh = {
      if(hdm.parallelism < 1) hdm.withParallelism(defParallel)
      else hdm
    }.withPartitionNum(followingParallel)
    if(hdm.children == null || hdm.children.isEmpty){
      Seq{nh}
    } else {
      val subHDMs = hdm.children.map( h => dftAccess(h, defParallel, nh.parallelism)).flatten
      subHDMs :+ nh
    }
  }
}