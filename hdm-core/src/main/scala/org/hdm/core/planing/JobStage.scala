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
                    isLocal:Boolean) extends  Serializable{

}
