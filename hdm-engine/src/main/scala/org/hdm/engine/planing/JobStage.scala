package org.hdm.core.planing

import org.hdm.core.model.HDM

import scala.collection.mutable

/**
 * Created by tiantian on 12/05/16.
 */
case class JobStage(appId:String,
                    jobId:String,
                    parents:mutable.Buffer[JobStage],
                    job:HDM[_],
                    context:String,
                    parallelism:Int,
                    isLocal:Boolean) extends  Serializable {

  def toJobStageInfo() = {
    val parentIDs = if(parents ne null) parents.map(_.jobId) else Seq.empty[String]
    JobStageInfo(appId, jobId, parentIDs, job.func.toString, context, parallelism, isLocal)
  }
}


case class JobStageInfo(appId:String,
                        jobId:String,
                        parents:Seq[String],
                        jobFunc:String,
                        context:String,
                        parallelism:Int,
                        isLocal:Boolean) extends  Serializable {

}
