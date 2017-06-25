package org.hdm.core.planing

import org.hdm.core.model.HDM
import org.hdm.core.server.HDMServerContext

import scala.collection.mutable

/**
 * Created by tiantian on 12/05/16.
 */

trait MultiClusterPlaner extends HDMPlaner {

  def planStages(hdm:HDM[_], parallelism:Int):Seq[JobStage]

  def plan(hdm:HDM[_], parallelism:Int):HDMPlans

  def hDMcontext:HDMServerContext

}

class StaticMultiClusterPlanner(hdmPlanner:HDMPlaner, val hDMcontext: HDMServerContext) extends MultiClusterPlaner {

  override def planStages(hdm: HDM[_], parallelism: Int): Seq[JobStage] = {
    val context = hDMcontext.leaderPath.get()
    // initial plan
    val isLocal = (hdm.appContext.masterPath == context)
    val appId = hdm.appContext.appName + "#" + hdm.appContext.version
    val lastStage = JobStage(appId, hdm.id, mutable.Buffer.empty[JobStage], hdm, hdm.appContext.masterPath, parallelism, isLocal)
    //plan for children
    val plan = dfsPlaning(context, hdm, lastStage, parallelism)
    plan :+ lastStage
  }

  private def dfsPlaning(context:String, curHDM:HDM[_], lastStage:JobStage, parallelism:Int) : Seq[JobStage] = {

    if (curHDM.children != null) {
      // curHDM has children
      val subParallelism = Math.max(parallelism / curHDM.children.size, 1)
      if (!curHDM.children.forall(c => c.appContext.masterPath == context)) {
        //not all the children are in the same context, then separate them as different stages
        val subPlans = for (child <- curHDM.children) yield {
          val appId = child.appContext.appName + "#" + child.appContext.version
          if(child.appContext.masterPath == context){
            val nStage = JobStage(appId, child.id, mutable.Buffer.empty[JobStage], child, child.appContext.masterPath, subParallelism, true)
            lastStage.parents += nStage
            // continue planning for local hdm
            dfsPlaning(context, child, nStage, subParallelism) :+ nStage
          } else {
            val nStage = JobStage(appId, child.id, mutable.Buffer.empty[JobStage], child, child.appContext.masterPath, subParallelism, false)
            lastStage.parents += nStage
            Seq(nStage)
          }
        }
        subPlans.flatten
      } else {
        // all children are in current context, then continue
        val subPlans = for (child <- curHDM.children) yield {
          dfsPlaning(context, child, lastStage, subParallelism)
        }
        subPlans.flatten
      }
    } else {
      // curHDM does not have children
      Seq.empty[JobStage]
    }
  }

  override def plan(hdm: HDM[_], parallelism: Int): HDMPlans = hdmPlanner.plan(hdm, parallelism)

}


object MultiClusterPlanner {

}
