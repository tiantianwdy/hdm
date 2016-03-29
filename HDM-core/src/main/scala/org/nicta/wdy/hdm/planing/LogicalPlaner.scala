package org.nicta.wdy.hdm.planing

import org.nicta.wdy.hdm.executor.HDMContext
import org.nicta.wdy.hdm.model.{AbstractHDM, NToN, NToOne, HDM}

/**
 * Created by tiantian on 7/01/15.
 */
trait LogicalPlaner extends Serializable{
  def plan(hdm:AbstractHDM[_], parallelism:Int):Seq[AbstractHDM[_]]
}


/**
 *
 */
class DefaultLocalPlaner(val cpuParallelFactor :Int = HDMContext.PLANER_PARALLEL_CPU_FACTOR,
                         val networkParallelFactor :Int = HDMContext.PLANER_PARALLEL_NETWORK_FACTOR ) extends LogicalPlaner{


  override def plan(hdm:AbstractHDM[_], parallelism:Int = 4):Seq[AbstractHDM[_]] = {
    dftAccess(hdm, parallelism, 1)
  }


  private def dftAccess(hdm:AbstractHDM[_], defParallel:Int, followingParallel:Int):Seq[AbstractHDM[_]]=  {
    val newHead = {
      if(hdm.parallelism < 1) {
        val parallelism = if (hdm.dependency == NToOne || hdm.dependency == NToN)
          defParallel * networkParallelFactor
        else defParallel * cpuParallelFactor
        hdm.withParallelism(parallelism)
      }
      else hdm
    }.withPartitionNum(followingParallel)

    if(hdm.children == null || hdm.children.isEmpty){
      Seq{newHead}
    } else {
      val subHDMs = hdm.children.map( h => dftAccess(h, defParallel, newHead.parallelism)).flatten
      subHDMs :+ newHead
    }
  }
}