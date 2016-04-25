package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.model._

/**
 * Created by tiantian on 7/01/15.
 */
trait LogicalPlaner extends Serializable{
  def plan(hdm:HDM[_], parallelism:Int):Seq[HDM[_]]
}


/**
 *
 */
class DefaultLocalPlaner(val cpuParallelFactor :Int = HDMContext.PLANER_PARALLEL_CPU_FACTOR,
                         val networkParallelFactor :Int = HDMContext.PLANER_PARALLEL_NETWORK_FACTOR ) extends LogicalPlaner{


  override def plan(hdm:HDM[_], parallelism:Int = 4):Seq[HDM[_]] = {
    dftAccess(hdm, parallelism, 1)
  }


  private def dftAccess(hdm:HDM[_], defParallel:Int, followingParallel:Int):Seq[HDM[_]]=  {
    val newHead = {
      if(hdm.parallelism < 1) {
        val parallelism = if (hdm.dependency == NToOne || hdm.dependency == NToN)
          defParallel * networkParallelFactor
        else defParallel * cpuParallelFactor
        hdm.withParallelism(parallelism)
      }
      else hdm
    }.withPartitionNum(followingParallel)
    hdm match {
      case h:ParHDM[_,_] =>
        if(hdm.children == null || hdm.children.isEmpty){
          Seq{newHead}
        } else {
          val subHDMs = hdm.children.map( h => dftAccess(h, defParallel, newHead.parallelism)).flatten
          subHDMs :+ newHead
        }
      case dh:DualDFM[_, _, _] =>
        if(dh.input1 == null || dh.input1.isEmpty || dh.input2 == null || dh.input2.isEmpty ){
          Seq{newHead}
        } else {
          val input1 = dh.input1.map( h => dftAccess(h, defParallel, newHead.parallelism)).flatten
          val input2 = dh.input2.map( h => dftAccess(h, defParallel, newHead.parallelism)).flatten
          input1 ++ input2 :+ newHead
        }
    }

  }
}